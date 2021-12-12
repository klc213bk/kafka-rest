package com.transglobe.streamingetl.kafka.rest.service;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.kafka.rest.bean.LastLogminerScn;

@Service
public class KafkaService {
	static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);

	static final int KAFKA_SERVER_PORT = 9092;
	static final int ZOOKEEPER_SERVER_PORT = 2181;
	
	@Value("${kafka.server.home}")
	private String kafkaServerHome;

	@Value("${zookeeper.start.script}")
	private String zookeeperStartScript;

	@Value("${zookeeper.start.properties}")
	private String zookeeperStartProperties;

	@Value("${zookeeper.stop.script}")
	private String zookeeperStopScript;

	@Value("${kafka.start.script}")
	private String kafkaStartScript;

	@Value("${kafka.start.properties}")
	private String kafkaStartProperties;

	@Value("${kafka.stop.script}")
	private String kafkaStopScript;

	@Value("${kafka.topics.script}")
	private String kafkaTopicsScript;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;

	private Process zookeeperStartProcess;
	private ExecutorService zookeeperStartExecutor;
	private AtomicBoolean zookeeperStartFinished = new AtomicBoolean(false);
	private AtomicBoolean zookeeperStopFinished = new AtomicBoolean(false);
	private Process zookeeperStopProcess;

	private Process kafkaStartProcess;
	private ExecutorService kafkaStartExecutor;
	private AtomicBoolean kafkaStartFinished = new AtomicBoolean(false);
	private AtomicBoolean kafkaStopFinished = new AtomicBoolean(false);
	private Process kafkaStopProcess;

	private Process listTopicsProcess;

	private Process createTopicProcess;

	private Process deleteTopicProcess;

	@PreDestroy
	public void destroy() {
		LOG.info(">>>> PreDestroy Kafka Service....");
		destroyKafka();
		destroyZookeeper();
	}

	public void startZookeeper() throws Exception {
		LOG.info(">>>>>>>>>>>> KafkaService.startZookeeper starting");
		try {
			if (zookeeperStartProcess == null || !zookeeperStartProcess.isAlive()) {
				LOG.info(">>>>>>>>>>>> zookeeperStartProcess.isAlive={} ", (zookeeperStartProcess == null)? null : zookeeperStartProcess.isAlive());
				zookeeperStartFinished.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(zookeeperStartScript, zookeeperStartProperties);

				builder.directory(new File(kafkaServerHome));
				zookeeperStartProcess = builder.start();

				zookeeperStartExecutor = Executors.newSingleThreadExecutor();
				zookeeperStartExecutor.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(zookeeperStartProcess.getInputStream()));
						reader.lines().forEach(line -> {
							LOG.info("********"+line);
						
						});
					}

				});

				while (!checkPortListening(ZOOKEEPER_SERVER_PORT)) {
					Thread.sleep(1000);
					LOG.info(">>>> Sleep for 1 second");;
				}

				LOG.info(">>>>>>>>>>>> KafkaService.startZookeeper End");
			} else {
				LOG.warn(" >>> zookeeperStartProcess is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, startZookeeper, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void startKafka() throws Exception {
		LOG.info(">>>>>>>>>>>> KafkaService.startKafka starting");
		try {
			if (kafkaStartProcess == null || !kafkaStartProcess.isAlive()) {
				LOG.info(">>>>>>>>>>>> kafkaStartProcess.isAlive={} ", (kafkaStartProcess == null)? null : kafkaStartProcess.isAlive());
				kafkaStartFinished.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(kafkaStartScript, kafkaStartProperties);

				builder.directory(new File(kafkaServerHome));
				kafkaStartProcess = builder.start();

				kafkaStartExecutor = Executors.newSingleThreadExecutor();
				kafkaStartExecutor.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(kafkaStartProcess.getInputStream()));
						reader.lines().forEach(line -> {
							LOG.info(line);
							
						});
					}

				});

				while (!checkPortListening(KAFKA_SERVER_PORT)) {
					Thread.sleep(1000);
					LOG.info(">>>> Sleep for 1 second");;
				}
				LOG.info(">>>>>>>>>>>> KafkaService.startKafka End");
			} else {
				LOG.warn(" >>> kafkaStartProcess is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, startKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopZookeeper() throws Exception {
		LOG.info(">>>>>>>>>>>> KafkaService.stopZookeeper starting...");
		try {
			if (zookeeperStopProcess == null || !zookeeperStopProcess.isAlive()) {
				LOG.info(">>>>>>>>>>>> zookeeperStopProcess.isAlive={} ", (zookeeperStopProcess == null)? null : zookeeperStopProcess.isAlive());
				zookeeperStopFinished.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(zookeeperStopScript);

				builder.directory(new File(kafkaServerHome));
				zookeeperStopProcess = builder.start();

				int exitVal = zookeeperStopProcess.waitFor();
				if (exitVal == 0) {
					zookeeperStopFinished.set(true);
					LOG.info(">>> Success!!! stopZookeeper, exitVal={}", exitVal);
				} else {
					LOG.error(">>> Error!!! stopZookeeper, exitcode={}", exitVal);
					zookeeperStopFinished.set(true);
				}

				while (checkPortListening(ZOOKEEPER_SERVER_PORT)) {
					Thread.sleep(1000);
					LOG.info(">>>> Sleep for 1 second");;
				}

				if (!zookeeperStopProcess.isAlive()) {
					zookeeperStopProcess.destroy();
				}

				LOG.info(">>>>>>>>>>>> KafkaService.stopZookeeper End");
			} else {
				LOG.warn(" >>> zookeeperStopProcess is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, stopZookeeper, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopKafka() throws Exception {
		LOG.info(">>>>>>>>>>>> KafkaService.stopKafka starting...");
		try {
			if (kafkaStopProcess == null || !kafkaStopProcess.isAlive()) {
				LOG.info(">>>>>>>>>>>> kafkaStopProcess.isAlive={} ", (kafkaStopProcess == null)? null : kafkaStopProcess.isAlive());
				kafkaStopFinished.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(kafkaStopScript);

				builder.directory(new File(kafkaServerHome));
				kafkaStopProcess = builder.start();

				int exitVal = kafkaStopProcess.waitFor();
				if (exitVal == 0) {
					LOG.info(">>> Success!!! stopKafka, exitVal={}", exitVal);
				} else {
					LOG.error(">>> Error!!! stopKafka, exitcode={}", exitVal);
					kafkaStopFinished.set(true);
				}
				long t0 = System.currentTimeMillis();
				
				while (checkPortListening(KAFKA_SERVER_PORT)) {
					Thread.sleep(1000);
					LOG.info(">>>> Sleep for 1 second");;
				}

				if (!kafkaStopProcess.isAlive()) {
					kafkaStopProcess.destroy();
				}

				LOG.info(">>>>>>>>>>>> KafkaService.stopKafka End");
			} else {
				LOG.warn(" >>> kafkaStopProcess is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, stopKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public Boolean isZookeeperAlive() throws Exception {
		if (zookeeperStartProcess == null) {
			return Boolean.FALSE;
		} else if (zookeeperStartProcess.isAlive()) {
			return Boolean.TRUE;
		} else {
			return Boolean.FALSE;
		}
	}
	public Boolean isKafkaAlive() throws Exception {
		if (kafkaStartProcess == null) {
			return Boolean.FALSE;
		} else if (kafkaStartProcess.isAlive()) {
			return Boolean.TRUE;
		} else {
			return Boolean.FALSE;
		}
	}
	private void destroyZookeeper() {
		if (zookeeperStartProcess != null) {
			LOG.warn(" >>> zookeeperStartpProcess ");
			LOG.warn(" >>> zookeeperStartProcess is aliv= {}", zookeeperStartProcess.isAlive());
			if (zookeeperStartProcess.isAlive()) {
				LOG.warn(" >>> destroy zookeeperStartProcess .");
				zookeeperStartProcess.destroy();

			}
		}
		if (zookeeperStopProcess != null) {
			LOG.warn(" >>> zookeeperStopProcess ");
			LOG.warn(" >>> zookeeperStopProcess is aliv= {}", zookeeperStopProcess.isAlive());
			if (zookeeperStopProcess.isAlive()) {
				LOG.warn(" >>> destroy zookeeperStopProcess .");
				zookeeperStopProcess.destroy();

			}
		}
		LOG.warn(" >>> zookeeperStartExecutor ...", zookeeperStartExecutor);
		if (zookeeperStartExecutor != null) {
			LOG.warn(" >>> shutdown zookeeperStartExecutor .");

			zookeeperStartExecutor.shutdown();

			LOG.warn(" >>> zookeeperStartExecutor isTerminated={} ", zookeeperStartExecutor.isTerminated());

			if (!zookeeperStartExecutor.isTerminated()) {
				LOG.warn(" >>> shutdownNow zookeeperStartExecutor .");
				zookeeperStartExecutor.shutdownNow();

				try {
					LOG.warn(" >>> awaitTermination zookeeperStartExecutor .");
					zookeeperStartExecutor.awaitTermination(600, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}
		}

	}
	private void destroyKafka() {
		if (kafkaStartProcess != null) {
			LOG.warn(" >>> kafkaStartProcess ");
			LOG.warn(" >>> kafkaStartProcess is aliv= {}", kafkaStartProcess.isAlive());
			if (kafkaStartProcess.isAlive()) {
				LOG.warn(" >>> destroy kafkaStartProcess .");
				kafkaStartProcess.destroy();

			}
		}
		if (kafkaStopProcess != null) {
			LOG.warn(" >>> kafkaStopProcess ");
			LOG.warn(" >>> kafkaStopProcess is aliv= {}", kafkaStopProcess.isAlive());
			if (kafkaStopProcess.isAlive()) {
				LOG.warn(" >>> destroy kafkaStopProcess .");
				kafkaStopProcess.destroy();

			}
		}
		LOG.warn(" >>> kafkaStartExecutor ...", kafkaStartExecutor);
		if (kafkaStartExecutor != null) {
			LOG.warn(" >>> shutdown kafkaStartExecutor .");

			kafkaStartExecutor.shutdown();

			LOG.warn(" >>> kafkaStartExecutor isTerminated={} ", kafkaStartExecutor.isTerminated());

			if (!kafkaStartExecutor.isTerminated()) {
				LOG.warn(" >>> shutdownNow kafkaStartExecutor .");
				kafkaStartExecutor.shutdownNow();

				try {
					LOG.warn(" >>> awaitTermination kafkaStartExecutor .");
					kafkaStartExecutor.awaitTermination(600, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}
		}

	}

	public Set<String> listTopics() throws Exception {
		LOG.info(">>>>>>>>>>>> listTopics ");
		List<String> topics = new ArrayList<String>();
		try {
			if (listTopicsProcess == null || !listTopicsProcess.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				String script = "./bin/kafka-topics.sh" + " --list --bootstrap-server " + kafkaBootstrapServer;
				builder.command("sh", "-c", script);
				//				builder.command(kafkaTopicsScript + " --list --bootstrap-server " + kafkaBootstrapServer);

				//				builder.command(kafkaTopicsScript, "--list", "--bootstrap-server", kafkaBootstrapServer);

				builder.directory(new File(kafkaServerHome));
				listTopicsProcess = builder.start();

				ExecutorService listTopicsExecutor = Executors.newSingleThreadExecutor();
				listTopicsExecutor.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(listTopicsProcess.getInputStream()));
						reader.lines().forEach(topic -> topics.add(topic));
					}

				});
				int exitVal = listTopicsProcess.waitFor();
				if (exitVal == 0) {

					LOG.info(">>> Success!!! listTopics, exitVal={}", exitVal);
				} else {
					LOG.error(">>> Error!!! listTopics, exitcode={}", exitVal);
					String errStr = (topics.size() > 0)? topics.get(0) : "";
					throw new Exception(errStr);
				}


			} else {
				LOG.warn(" >>> listTopics is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, listTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			LOG.error(">>> Error!!!, listTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		}
		return new HashSet<>(topics);
	}

	public void createTopic(String topic) throws Exception {
		LOG.info(">>>>>>>>>>>> createTopic topic=={}", topic);
		try {
			if (createTopicProcess == null || !createTopicProcess.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				String script = "./bin/kafka-topics.sh --create --bootstrap-server " + kafkaBootstrapServer + " --replication-factor 1 --partitions 1 --topic " + topic;
				builder.command("sh", "-c", script);

				builder.directory(new File(kafkaServerHome));
				createTopicProcess = builder.start();

				int exitVal = createTopicProcess.waitFor();
				if (exitVal == 0) {
					LOG.info(">>> Success!!! createTopic:{}, exitcode={}", topic, exitVal);
				} else {
					LOG.error(">>> Error!!! createTopic:{}, exitcode={}", topic, exitVal);
				}
				LOG.info(">>> createTopicProcess isalive={}", createTopicProcess.isAlive());
				if (!createTopicProcess.isAlive()) {
					createTopicProcess.destroy();
				}


				//				if (!createTopicProcess.isAlive()) {
				//					LOG.info(">>>  createTopicProcess isAlive={}", createTopicProcess.isAlive());
				//					createTopicExecutor.shutdown();
				//					if (!createTopicExecutor.isTerminated()) {
				//						LOG.info(">>> createTopicExecutor is not Terminated, prepare to shutdown executor");
				//						createTopicExecutor.shutdownNow();
				//
				//						try {
				//							createTopicExecutor.awaitTermination(600, TimeUnit.SECONDS);
				//						} catch (InterruptedException e) {
				//							LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
				//									ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				//						}
				//
				//					} else {
				//						LOG.warn(">>> createTopicExecutor is already terminated");
				//					}
				//				} else {
				//					LOG.info(">>>  createTopicProcess isAlive={}, destroy it", createTopicProcess.isAlive());
				//					createTopicProcess.destroy();
				//				}

			} else {
				LOG.warn(" >>> createTopic is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, createTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			LOG.error(">>> Error!!!, createTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void deleteAllTopics() throws Exception {
		Set<String> topicSet = listTopics();
		
		for (String topic : topicSet) {
			deleteTopic(topic);
		}
	}
	public void deleteTopic(String topic) throws Exception {
		LOG.info(">>>>>>>>>>>> deleteTopic topic=={}", topic);
		try {
			if (deleteTopicProcess == null || !deleteTopicProcess.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				String script = "./bin/kafka-topics.sh --delete --bootstrap-server " + kafkaBootstrapServer + " --topic " + topic;
				builder.command("sh", "-c", script);

				builder.directory(new File(kafkaServerHome));
				deleteTopicProcess = builder.start();

				int exitVal = deleteTopicProcess.waitFor();
				if (exitVal == 0) {
					LOG.info(">>> Success!!! deleteTopic:{}, exitcode={}", topic, exitVal);
				} else {
					LOG.error(">>> Error!!! deleteTopic:{}, exitcode={}", topic, exitVal);
				}
				LOG.info(">>> deleteTopicProcess isalive={}", deleteTopicProcess.isAlive());
				if (!deleteTopicProcess.isAlive()) {
					deleteTopicProcess.destroy();
				}


				//				if (!createTopicProcess.isAlive()) {
				//					LOG.info(">>>  createTopicProcess isAlive={}", createTopicProcess.isAlive());
				//					createTopicExecutor.shutdown();
				//					if (!createTopicExecutor.isTerminated()) {
				//						LOG.info(">>> createTopicExecutor is not Terminated, prepare to shutdown executor");
				//						createTopicExecutor.shutdownNow();
				//
				//						try {
				//							createTopicExecutor.awaitTermination(600, TimeUnit.SECONDS);
				//						} catch (InterruptedException e) {
				//							LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
				//									ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				//						}
				//
				//					} else {
				//						LOG.warn(">>> createTopicExecutor is already terminated");
				//					}
				//				} else {
				//					LOG.info(">>>  createTopicProcess isAlive={}, destroy it", createTopicProcess.isAlive());
				//					createTopicProcess.destroy();
				//				}

			} else {
				LOG.warn(" >>> deleteTopic is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			LOG.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}


	public Optional<LastLogminerScn> getEbaoKafkaLastLogminerScn() {
		LOG.info(">>>>>kafkaBootstrapServer={}",kafkaBootstrapServer);
		Consumer<String, String> consumer = createConsumer(kafkaBootstrapServer, "ebao-getLogminerLastScn");

		LastLogminerScn lastLogminer = null;
		List<TopicPartition> tps = new ArrayList<>();
		Map<String, List<PartitionInfo>> map = consumer.listTopics();
		for (String topic : map.keySet()) {	
			if (topic.startsWith("EBAOPRD1")) {
				for (PartitionInfo pi : map.get(topic)) {
					tps.add(new TopicPartition(pi.topic(), pi.partition()));
				}

			}
		}
		consumer.assign( tps);

		long lastScn = 0L;
		long lastCommitScn = 0L;
		Map<TopicPartition, Long> offsetMap = consumer.endOffsets(tps);
		for (TopicPartition tp : offsetMap.keySet()) {
			//			long position = consumer.position(tp);
			long offset = offsetMap.get(tp);

			if (offset == 0) {
				continue;
			}
			LOG.info("topic:{}, partition:{},offset:{}", tp.topic(), tp.partition(), offset);
			consumer.seek(tp, offset- 1);

			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			System.out.println("record count:" + consumerRecords.count());

			for (ConsumerRecord<String, String> record : consumerRecords) {
				LOG.info("record key:{}, topic:{}, partition:{},offset:{}, timestamp:{}",
						record.key(), record.topic(), record.partition(), record.offset(), record.timestamp());
				//				System.out.println("Record Key " + record.key());
				//				System.out.println("Record value " + record.value());
				//				System.out.println("Record topic " + record.topic() + " partition " + record.partition());
				//				System.out.println("Record offset " + record.offset() + " timestamp " + record.timestamp());

				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

				try {
					JsonNode jsonNode = objectMapper.readTree(record.value());
					JsonNode payload = jsonNode.get("payload");
					//	payloadStr = payload.toString();

					String operation = payload.get("OPERATION").asText();
					String tableName = payload.get("TABLE_NAME").asText();
					Long scn = Long.valueOf(payload.get("SCN").asText());
					Long commitScn = Long.valueOf(payload.get("COMMIT_SCN").asText());
					String rowId = payload.get("ROW_ID").asText();
					Long t = Long.valueOf(payload.get("TIMESTAMP").asText());
					LOG.info("operation:{},tableName:{},scn:{}, commitScn:{}, rowId:{},timestamp",operation,tableName,scn,commitScn,rowId,t);

					if (scn.longValue() > lastScn) {
						lastScn = scn.longValue();
					}
					if (commitScn.longValue() > lastCommitScn) {
						lastCommitScn = commitScn.longValue();
					}
				} catch(Exception e) {
					e.printStackTrace();
				} 
			}

		}
		consumer.close();

		lastLogminer = new LastLogminerScn(lastScn, lastCommitScn);
		return Optional.of(lastLogminer);
	}
	
	private Consumer<String, String> createConsumer(String kafkaBootstrapServer, String kafkaGroupId) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		Consumer<String, String> consumer = new KafkaConsumer<>(props);

		return consumer;
	}
	
	private boolean checkPortListening(int port) throws Exception {
		LOG.info(">>>>>>>>>>>> checkPortListening:{} ", port);

		BufferedReader reader = null;
		try {
			ProcessBuilder builder = new ProcessBuilder();
			String script = "netstat -tnlp | grep :" + port;
			builder.command("bash", "-c", script);
			//				builder.command(kafkaTopicsScript + " --list --bootstrap-server " + kafkaBootstrapServer);

			//				builder.command(kafkaTopicsScript, "--list", "--bootstrap-server", kafkaBootstrapServer);

			builder.directory(new File("."));
			Process checkPortProcess = builder.start();

			AtomicBoolean portRunning = new AtomicBoolean(false);


			int exitVal = checkPortProcess.waitFor();
			if (exitVal == 0) {
				reader = new BufferedReader(new InputStreamReader(checkPortProcess.getInputStream()));
				reader.lines().forEach(line -> {
					if (StringUtils.contains(line, "LISTEN")) {
						portRunning.set(true);
						LOG.info(">>> Success!!! portRunning.set(true)");
					}
				});
				reader.close();

				LOG.info(">>> Success!!! portRunning={}", portRunning.get());
			} else {
				LOG.error(">>> Error!!!  exitcode={}", exitVal);


			}
			if (checkPortProcess.isAlive()) {
				checkPortProcess.destroy();
			}

			return portRunning.get();
		} finally {
			if (reader != null) reader.close();
		}



	}
}
//	@Value("${kafka.home}")
//	private String kafkaHome;
//
//	@Value("${streamingetl.home}")kafkaStartExecutor = Executors.newSingleThreadExecutor();
//kafkaStartExecutor.submit(new Runnable() {
//
//	@Override
//	public void run() {
//		BufferedReader reader = new BufferedReader(new InputStreamReader(kafkaStartProcess.getInputStream()));
//		reader.lines().forEach(line -> {
//			LOG.info("+++++++++++++++" + line);
//			if (line.contains("Loading group metadata")) {
//				kafkaStartFinished.set(true);
//			} else if (line.contains("shut down completed")) {
//				kafkaStopFinished.set(true);
//			}
//		});
//	}
//});
//while (!kafkaStartFinished.get()) {
//	LOG.info(">>>>>>WAITING 1 sec FOR FINISH");
//	Thread.sleep(1000);
//}
//	private String streamingetlHome;
//
//	@Value("${kafka.bootstrap.server}")
//	private String kafkaBootstrapServer;
//
//	@Value("${kafka.streamingetl.group.id}")
//	private String kafkaStreamingetlGroupId;
//
//	@Value("${kafka.topic.startwith.ebaoprd}")
//	private String kafkaTopicStartwithEbaoprd;
//
//	@Value("${zookeeper.port}")
//	private String zookeeperPort;
//
//	@Value("${kafka.port}")
//	private String kafkaPort;
//
//	@Value("${connect.rest.url}")
//	private String connectRestUrl;
//
//	private Process zookeeperStartProcess;
//	private ExecutorService zookeeperStartExecutor;
//	private AtomicBoolean zookeeperStartFinished = new AtomicBoolean(false);
//	private AtomicBoolean zookeeperStopFinished = new AtomicBoolean(false);
//
//	private Process zookeeperStopProcess;
//
//	private Process kafkaStartProcess;
//	private ExecutorService kafkaStartExecutor;
//	private AtomicBoolean kafkaStartFinished = new AtomicBoolean(false);
//	private AtomicBoolean kafkaStopFinished = new AtomicBoolean(false);
//
//	private Process kafkaStopProcess;
//
//	private Process listTopicsProcess;
//	private ExecutorService listTopicsExecutor;
//
//	private Process deleteTopicProcess;
//	private ExecutorService deleteTopicExecutor;
//
//	private Process createTopicProcess;
//	private ExecutorService createTopicExecutor;
//
//	public void startZookeeper() throws Exception {
//		LOG.info(">>>>>>>>>>>> startZookeeper ");
//		try {
//			if (zookeeperStartProcess == null || !zookeeperStartProcess.isAlive()) {
//				LOG.info(">>>>>>>>>>>> zookeeperStartProcess.isAlive={} ", (zookeeperStartProcess == null)? null : zookeeperStartProcess.isAlive());
//				zookeeperStartFinished.set(false);
//				ProcessBuilder builder = new ProcessBuilder();
//				String script = "./bin/start-kafka-zookeeper.sh";
//				//builder.command("sh", "-c", script);
//				builder.command(script);
//
//				builder.directory(new File(streamingetlHome));
//				zookeeperStartProcess = builder.start();
//
//				zookeeperStartExecutor = Executors.newSingleThreadExecutor();
//				zookeeperStartExecutor.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						BufferedReader reader = new BufferedReader(new InputStreamReader(zookeeperStartProcess.getInputStream()));
//						reader.lines().forEach(line -> {
//							LOG.info("********"+line);
//							if (line.contains("Using checkIntervalMs")) {
//								zookeeperStartFinished.set(true);
//							} 
//						});
//					}
//
//				});
//
//				while (!zookeeperStartFinished.get()) {
//					LOG.info(">>>>>>WAITING 1 sec FOR FINISH");
//					Thread.sleep(1000);
//				}
//
//				Runtime.getRuntime().addShutdownHook(new Thread() {
//					@Override
//					public void run() {
//
//						shutdownKafkaServer();
//
//					}
//				});
//
//			} else {
//				LOG.warn(" >>> zookeeperProcess is currently Running.");
//			}
//		} catch (IOException e) {
//			LOG.error(">>> Error!!!, startZookeeper, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//			throw e;
//		} 
//	}
//	public void stopZookeeper() throws Exception {
//		LOG.info(">>>>>>>>>>>> stopZookeeper ");
//		try {
//			if (zookeeperStopProcess == null || !zookeeperStopProcess.isAlive()) {
//				LOG.info(">>>>>>>>>>>> zookeeperStopProcess.isAlive={} ", (zookeeperStopProcess == null)? null : zookeeperStopProcess.isAlive());
//				zookeeperStopFinished.set(false);
//				ProcessBuilder builder = new ProcessBuilder();
//				String script = "./bin/stop-kafka-zookeeper.sh";
//				//		builder.command("sh", "-c", script);
//				builder.command(script);
//
//				builder.directory(new File(streamingetlHome));
//				zookeeperStopProcess = builder.start();
//
//				int exitVal = zookeeperStopProcess.waitFor();
//				if (exitVal == 0) {
//					zookeeperStopFinished.set(true);
//					LOG.info(">>> Success!!! stopZookeeper, exitVal={}", exitVal);
//				} else {
//					LOG.error(">>> Error!!! stopZookeeper, exitcode={}", exitVal);
//				}
//
//				while (!zookeeperStopFinished.get()) {
//					LOG.info(">>>>>>WAITING 1 sec FOR FINISH");
//					Thread.sleep(1000);
//				}
//
//				//kill zookeeper start
//				LOG.warn(" >>> zookeeperStartProcess .isAlive..", zookeeperStartProcess.isAlive());
//				if (zookeeperStartProcess != null) {
//					LOG.warn(" >>> destroy zookeeperStartProcess .");
//					zookeeperStartProcess.destroy();
//				}
//				LOG.warn(" >>> zookeeperStartExecutor ...", zookeeperStartExecutor);
//				if (zookeeperStartExecutor != null) {
//					LOG.warn(" >>> shutdown zookeeperStartExecutor .");
//
//					zookeeperStartExecutor.shutdown();
//
//					LOG.warn(" >>> zookeeperStartExecutor isTerminated={} ", zookeeperStartExecutor.isTerminated());
//
//					if (!zookeeperStartExecutor.isTerminated()) {
//						LOG.warn(" >>> shutdownNow zookeeperStartExecutor .");
//						zookeeperStartExecutor.shutdownNow();
//
//						try {
//							LOG.warn(" >>> awaitTermination zookeeperStartExecutor .");
//							zookeeperStartExecutor.awaitTermination(600, TimeUnit.SECONDS);
//						} catch (InterruptedException e) {
//							LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
//									ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//						}
//
//					}
//				}
//				LOG.warn(" >>> zookeeperStopProcess ");
//				LOG.warn(" >>> zookeeperStopProcess is aliv= {}", zookeeperStopProcess.isAlive());
//				if (zookeeperStopProcess.isAlive()) {
//					zookeeperStopProcess.destroy();
//
//				}
//
//			} else {
//				LOG.warn(" >>> zookeeperStopProcess is currently Running.");
//			}
//			
//			LOG.info(">>> zookeeper shutdowned !!");
//
//		} catch (IOException e) {
//			LOG.error(">>> Error!!!, stopZookeeper, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//			throw e;
//		} catch (InterruptedException e) {
//			LOG.error(">>> Error!!!, stopZookeeper, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//			throw e;
//		}
//	}
//	public void startKafka() throws Exception {
//		LOG.info(">>>>>>>>>>>> startKafka ");
//		try {
//			if (kafkaStartProcess == null || !kafkaStartProcess.isAlive()) {
//				LOG.info(">>>>>>>>>>>> kafkaStartProcess.isAlive={} ", (kafkaStartProcess == null)? null : kafkaStartProcess.isAlive());
//				kafkaStartFinished.set(false);
//				ProcessBuilder builder = new ProcessBuilder();
//				String script = "./bin/start-kafka-servers.sh";
//				//	builder.command("sh", "-c", script);
//				builder.command(script);
//
//				builder.directory(new File(streamingetlHome));
//				kafkaStartProcess = builder.start();
//
//				kafkaStartExecutor = Executors.newSingleThreadExecutor();
//				kafkaStartExecutor.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						BufferedReader reader = new BufferedReader(new InputStreamReader(kafkaStartProcess.getInputStream()));
//						reader.lines().forEach(line -> {
//							LOG.info("+++++++++++++++" + line);
//							if (line.contains("Loading group metadata")) {
//								kafkaStartFinished.set(true);
//							} else if (line.contains("shut down completed")) {
//								kafkaStopFinished.set(true);
//							}
//						});
//					}
//				});
//				while (!kafkaStartFinished.get()) {
//					LOG.info(">>>>>>WAITING 1 sec FOR FINISH");
//					Thread.sleep(1000);
//				}
//				
//				Runtime.getRuntime().addShutdownHook(new Thread() {
//					@Override
//					public void run() {
//
//						shutdownKafkaServer();
//
//					}
//				});
//				
//			} else {
//				LOG.warn(" >>> startKafkaProcess is currently Running.");
//			}
//		} catch (IOException e) {
//			LOG.error(">>> Error!!!, startKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//			throw e;
//		} 
//	}
//	public void stopKafka() throws Exception {
//		LOG.info(">>>>>>>>>>>> stopKafka ");
//		try {
//			if (kafkaStopProcess == null || !kafkaStopProcess.isAlive()) {
//				LOG.info(">>>>>>>>>>>> kafkaStopProcess.isAlive={} ", (kafkaStopProcess == null)? null : kafkaStopProcess.isAlive());
//				kafkaStopFinished.set(false);
//				ProcessBuilder builder = new ProcessBuilder();
//				String script = "./bin/stop-kafka-servers.sh";
//				//	builder.command("sh", "-c", script);
//				builder.command(script);
//
//				builder.directory(new File(streamingetlHome));
//				kafkaStopProcess = builder.start();
//
//
//				int exitVal = kafkaStopProcess.waitFor();
//				if (exitVal == 0) {
//					LOG.info(">>> Success!!! stopKafka, exitVal={}", exitVal);
//				} else {
//					LOG.error(">>> Error!!! stopKafka, exitcode={}", exitVal);
//					throw new Exception(">>>>>>>>>>>>>>>>>ERROR !!!!!");
//				}
//
//				while (!kafkaStopFinished.get()) {
//					LOG.info(">>>>>>WAITING 1 sec FOR FINISH");
//					Thread.sleep(1000);
//				}
//
//				LOG.info(">>>>>>>>>>>>>>>>>>KAFKA SERVER STOP !!!!!!");
//
//				// kill kafka start
//				LOG.warn(" >>> kafkaStartProcess .isAlive..", kafkaStartProcess.isAlive());
//				if (kafkaStartProcess != null) {
//					LOG.warn(" >>> destroy kafkaStartProcess .");
//					kafkaStartProcess.destroy();
//				}
//				LOG.warn(" >>> kafkaStartExecutor ...", kafkaStartExecutor);
//				if (kafkaStartExecutor != null) {
//					LOG.warn(" >>> shutdown kafkaStartExecutor .");
//
//					kafkaStartExecutor.shutdown();
//
//					LOG.warn(" >>> kafkaStartExecutor isTerminated={} ", kafkaStartExecutor.isTerminated());
//
//					if (!kafkaStartExecutor.isTerminated()) {
//						LOG.warn(" >>> shutdownNow kafkaStartExecutor .");
//						kafkaStartExecutor.shutdownNow();
//
//						try {
//							LOG.warn(" >>> awaitTermination kafkaStartExecutor .");
//							kafkaStartExecutor.awaitTermination(600, TimeUnit.SECONDS);
//						} catch (InterruptedException e) {
//							LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
//									ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//						}
//
//					}
//				}
//				LOG.warn(" >>> kafkaStopProcess ");
//				LOG.warn(" >>> kafkaStopProcess is aliv= {}", kafkaStopProcess.isAlive());
//				if (kafkaStopProcess.isAlive()) {
//					kafkaStopProcess.destroy();
//
//				}
//
//			} else {
//				LOG.warn(" >>> stopKafkaProcess is currently Running.");
//			}
////
////			// kill process
////			ProcessBuilder builder = new ProcessBuilder();
////			String script = String.format("kill -9 $(lsof -t -i:%d -sTCP:LISTEN)", Integer.valueOf(kafkaPort));
////			LOG.info(">>> stop script={}", script);
////
////			builder.command("bash", "-c", script);
////			//builder.command(script);
////
////			//	builder.directory(new File(streamingetlHome));
////			Process schedulerShutdownProcess = builder.start();
////			int exitVal = schedulerShutdownProcess.waitFor();
////			if (exitVal == 0) {
////				LOG.info(">>> Success!!! kill kafka ");
////			} else {
////				LOG.error(">>> Error!!! kill kafka, exitcode={}", exitVal);
////			}
//
//			LOG.info(">>> kafka shutdowned !!");
//		} catch (IOException e) {
//			LOG.error(">>> Error!!!, stopKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//			throw e;
//		} catch (InterruptedException e) {
//			LOG.error(">>> Error!!!, stopKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//			throw e;
//		}
//	}

//	public void deleteTopic(String topic) throws Exception {
//		LOG.info(">>>>>>>>>>>> deleteTopic topic=={}", topic);
//		try {
//			if (deleteTopicProcess == null || !deleteTopicProcess.isAlive()) {
//				ProcessBuilder builder = new ProcessBuilder();
//				String script = "./bin/kafka-topics.sh --delete --bootstrap-server " + kafkaBootstrapServer + " --topic " + topic;
//				builder.command("sh", "-c", script);
//
//				builder.directory(new File(kafkaHome));
//				deleteTopicProcess = builder.start();
//
//				deleteTopicExecutor = Executors.newSingleThreadExecutor();
//				deleteTopicExecutor.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						BufferedReader reader = new BufferedReader(new InputStreamReader(deleteTopicProcess.getInputStream()));
//						reader.lines().forEach(str -> LOG.info(str));
//					}
//
//				});
//
//				int exitVal = deleteTopicProcess.waitFor();
//				if (exitVal == 0) {
//					LOG.info(">>> Success!!! deleteTopic:{}, exitcode={}", topic, exitVal);
//				} else {
//					LOG.error(">>> Error!!! deleteTopic:{}, exitcode={}", topic, exitVal);
//				}
//
//				//
//				//				if (!deleteTopicProcess.isAlive()) {
//				//					LOG.info(">>>  deleteTopicProcess isAlive={}", deleteTopicProcess.isAlive());
//				//					deleteTopicExecutor.shutdown();
//				//					if (!deleteTopicExecutor.isTerminated()) {
//				//						LOG.info(">>> deleteTopicExecutor is not Terminated, prepare to shutdown executor");
//				//						deleteTopicExecutor.shutdownNow();
//				//
//				//						try {
//				//							deleteTopicExecutor.awaitTermination(600, TimeUnit.SECONDS);
//				//						} catch (InterruptedException e) {
//				//							LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
//				//									ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//				//						}
//				//
//				//					} else {
//				//						LOG.warn(">>> deleteTopicExecutor is already terminated");
//				//					}
//				//				} else {
//				//					LOG.info(">>>  deleteTopicProcess isAlive={}, destroy it", deleteTopicProcess.isAlive());
//				//					deleteTopicProcess.destroy();
//				//				}
//			} else {
//				LOG.warn(" >>> deleteTopic is currently Running.");
//			}
//		} catch (IOException e) {
//			LOG.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//			throw e;
//		} catch (InterruptedException e) {
//			LOG.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//			throw e;
//		}
//	}

//	public Optional<LastLogminerScn> getKafkaLastLogminerScn() {
//		LOG.info(">>>>>kafkaBootstrapServer={}",kafkaBootstrapServer);
//		Consumer<String, String> consumer = createConsumer(kafkaBootstrapServer, kafkaStreamingetlGroupId+"-getLogminerLastScn");
//
//		LastLogminerScn lastLogminer = null;
//		List<TopicPartition> tps = new ArrayList<>();
//		Map<String, List<PartitionInfo>> map = consumer.listTopics();
//		for (String topic : map.keySet()) {	
//			if (topic.startsWith(kafkaTopicStartwithEbaoprd)) {
//				for (PartitionInfo pi : map.get(topic)) {
//					tps.add(new TopicPartition(pi.topic(), pi.partition()));
//				}
//
//			}
//		}
//		consumer.assign( tps);
//
//		long lastScn = 0L;
//		long lastCommitScn = 0L;
//		Map<TopicPartition, Long> offsetMap = consumer.endOffsets(tps);
//		for (TopicPartition tp : offsetMap.keySet()) {
//			//			long position = consumer.position(tp);
//			long offset = offsetMap.get(tp);
//
//			if (offset == 0) {
//				continue;
//			}
//			LOG.info("topic:{}, partition:{},offset:{}", tp.topic(), tp.partition(), offset);
//			consumer.seek(tp, offset- 1);
//
//			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
//			System.out.println("record count:" + consumerRecords.count());
//
//			for (ConsumerRecord<String, String> record : consumerRecords) {
//				LOG.info("record key:{}, topic:{}, partition:{},offset:{}, timestamp:{}",
//						record.key(), record.topic(), record.partition(), record.offset(), record.timestamp());
//				//				System.out.println("Record Key " + record.key());
//				//				System.out.println("Record value " + record.value());
//				//				System.out.println("Record topic " + record.topic() + " partition " + record.partition());
//				//				System.out.println("Record offset " + record.offset() + " timestamp " + record.timestamp());
//
//				ObjectMapper objectMapper = new ObjectMapper();
//				objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
//
//				try {
//					JsonNode jsonNode = objectMapper.readTree(record.value());
//					JsonNode payload = jsonNode.get("payload");
//					//	payloadStr = payload.toString();
//
//					String operation = payload.get("OPERATION").asText();
//					String tableName = payload.get("TABLE_NAME").asText();
//					Long scn = Long.valueOf(payload.get("SCN").asText());
//					Long commitScn = Long.valueOf(payload.get("COMMIT_SCN").asText());
//					String rowId = payload.get("ROW_ID").asText();
//					Long t = Long.valueOf(payload.get("TIMESTAMP").asText());
//					LOG.info("operation:{},tableName:{},scn:{}, commitScn:{}, rowId:{},timestamp",operation,tableName,scn,commitScn,rowId,t);
//
//					if (scn.longValue() > lastScn) {
//						lastScn = scn.longValue();
//					}
//					if (commitScn.longValue() > lastCommitScn) {
//						lastCommitScn = commitScn.longValue();
//					}
//				} catch(Exception e) {
//					e.printStackTrace();
//				} 
//			}
//
//		}
//		consumer.close();
//
//		lastLogminer = new LastLogminerScn(lastScn, lastCommitScn);
//		return Optional.of(lastLogminer);
//	}
//	public Map<String,String> getConnectorConfig(String connectorName) throws Exception {
//		Map<String,String> configmap = new HashMap<>();
//		String urlStr = String.format(connectRestUrl+"/connectors/%s/config", connectorName);
//		LOG.info(">>>>>>>>>>>> urlStr={} ", urlStr);
//		HttpURLConnection httpCon = null;
//		try {
//			URL url = new URL(urlStr);
//			httpCon = (HttpURLConnection)url.openConnection();
//			httpCon.setRequestMethod("GET");
//			int responseCode = httpCon.getResponseCode();
//			String readLine = null;
//			//			if (httpCon.HTTP_OK == responseCode) {
//			BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
//			StringBuffer response = new StringBuffer();
//			while ((readLine = in.readLine()) != null) {
//				response.append(readLine);
//			}
//			in.close();
//
//			LOG.info(">>>>> CONNECT REST responseCode={},response={}", responseCode, response.toString());
//
//			configmap = new ObjectMapper().readValue(response.toString(), HashMap.class);
//
//		} finally {
//			if (httpCon != null ) httpCon.disconnect();
//		}
//		return configmap;
//
//	}
//	public List<String> getKafkaconnectors() throws Exception {
//		LOG.info(">>>>>>>>>>>> getKafkaconnectors ");
//		String urlStr = connectRestUrl+"/connectors";
//		HttpURLConnection httpCon = null;
//		ConnectorsWrapper wrapper = null;
//		try {
//			URL url = new URL(urlStr);
//			httpCon = (HttpURLConnection)url.openConnection();
//			httpCon.setRequestMethod("GET");
//			int responseCode = httpCon.getResponseCode();
//			String readLine = null;
//			//			if (httpCon.HTTP_OK == responseCode) {
//			BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
//			StringBuffer response = new StringBuffer();
//			while ((readLine = in.readLine()) != null) {
//				response.append(readLine);
//			}
//			in.close();
//
//			LOG.info(">>>>> CONNECT REST response", response.toString());
//
//			ObjectMapper objectMapper = new ObjectMapper();
//			wrapper = objectMapper.readValue(response.toString(), ConnectorsWrapper.class);
//
//			//				JsonNode jsonNode = objectMapper.readTree(response.toString());
//			//				Iterator<JsonNode> it = jsonNode.iterator();
//			//				while (it.hasNext()) {
//			//					JsonNode node = it.next();
//			//					connectorList.add(node.asText());
//			//				}
//			//
//			//				return connectorList;
//			//			} else {
//			//				LOG.error(">>> Response code={}", responseCode);
//			//				throw new Exception("Response code="+responseCode);
//			//			}
//		} finally {
//			if (httpCon != null ) httpCon.disconnect();
//		}
//		return (wrapper == null)? new ArrayList<>() : wrapper.getValues();
//	}
//	public String getConnectorStatus(String connector) throws Exception {
//		String urlStr = connectRestUrl+"/connectors/" + connector+ "/status";
//		HttpURLConnection httpCon = null;
//		//		ConnectorStatus connectorStatus = null;
//		try {
//			URL url = new URL(urlStr);
//			httpCon = (HttpURLConnection)url.openConnection();
//			httpCon.setRequestMethod("GET");
//			int responseCode = httpCon.getResponseCode();
//			LOG.info(">>>>> getConnectorStatus responseCode:" + responseCode);
//
//			String readLine = null;
//
//			BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream(), "UTF-8"));
//			StringBuffer response = new StringBuffer();
//			while ((readLine = in.readLine()) != null) {
//				response.append(readLine);
//			}
//			in.close();
//
//			LOG.info(">>>>> getConnectorStatus response:" + response.toString());
//
//			return response.toString();
//
//			//			ObjectMapper objectMapper = new ObjectMapper();
//			//			objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//			//			connectorStatus = objectMapper.readValue(response.toString(), ConnectorStatus.class);
//
//			//			JsonNode jsonNode = objectMapper.readTree(response.toString());
//			//			JsonNode connectorNode = jsonNode.get("connector");
//			//			JsonNode stateNode = connectorNode.get("state");
//			//			String state = stateNode.asText();
//			//
//			//			if (httpCon.HTTP_OK == responseCode) {
//			//				connectorStatus = new ConnectorStatus(connector, state, null);
//			//			} else {
//			//				connectorStatus = new ConnectorStatus(connector, state, response.toString());
//			//			}
//		} finally {
//			if (httpCon != null ) httpCon.disconnect();
//		}
//	}
//	public boolean pauseConnector(String connectorName) throws Exception{
//		String urlStr = connectRestUrl + "/connectors/" + connectorName + "/pause";
//		LOG.info(">>>>> connector urlStr:" + urlStr);
//
//		HttpURLConnection httpConn = null;
//		//		DataOutputStream dataOutStream = null;
//		int responseCode = -1;
//		try {
//			URL url = new URL(urlStr);
//			httpConn = (HttpURLConnection)url.openConnection();
//			httpConn.setRequestMethod("PUT");
//
//			responseCode = httpConn.getResponseCode();
//			LOG.info(">>>>> pause responseCode={}",responseCode);
//
//			String readLine = null;
//
//			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
//			StringBuffer response = new StringBuffer();
//			while ((readLine = in.readLine()) != null) {
//				response.append(readLine);
//			}
//			in.close();
//
//			LOG.info(">>>>> pause response={}",response.toString());
//
//			if (202 == responseCode) {
//				return true;
//			} else {
//				return false;
//			}
//		} finally {
//
//			if (httpConn != null )httpConn.disconnect();
//		}
//	}
//	public boolean deleteConnector(String connectorName) throws Exception{
//		String urlStr = connectRestUrl + "/connectors/" + connectorName +"/";
//		//		LOG.info(">>>>> connector urlStr:" + urlStr);
//		HttpURLConnection httpConn = null;
//		try {
//			URL url = new URL(urlStr);
//			httpConn = (HttpURLConnection)url.openConnection();
//			httpConn.setRequestMethod("DELETE");
//			int responseCode = httpConn.getResponseCode();
//
//			LOG.info(">>>>> DELETE responseCode={}",responseCode);
//
//			String readLine = null;
//
//			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
//			StringBuffer response = new StringBuffer();
//			while ((readLine = in.readLine()) != null) {
//				response.append(readLine);
//			}
//			in.close();
//
//			LOG.info(">>>>> delete response={}",response.toString());
//
//			if (204 == responseCode) {
//				return true;
//			} else {
//				return false;
//			}
//
//		} finally {
//			if (httpConn != null )httpConn.disconnect();
//		}
//	}
//
//
//	public boolean createConnector(String connectorName, Map<String, String> configmap) throws Exception {
//		LOG.info(">>>>>>>>>>>> createNewConnector");
//
//		HttpURLConnection httpConn = null;
//		DataOutputStream dataOutStream = null;
//		try {
//
//			//			Map<String, Object> map = new HashMap<>();
//			//			map.put("name", connectorName);
//			//			map.put("config", configmap);
//
//			ObjectMapper objectMapper = new ObjectMapper();
//			String configStr = objectMapper.writeValueAsString(configmap);
//
//
//			String urlStr = connectRestUrl+"/connectors/" + connectorName + "/config";
//
//			LOG.info(">>>>> connector urlStr={},reConfigStr={}", urlStr, configStr);
//
//			URL url = new URL(urlStr);
//			httpConn = (HttpURLConnection)url.openConnection();
//			httpConn.setRequestMethod("PUT"); 
//			httpConn.setDoInput(true);
//			httpConn.setDoOutput(true);
//			httpConn.setRequestProperty("Content-Type", "application/json");
//			httpConn.setRequestProperty("Accept", "application/json");
//
//			dataOutStream = new DataOutputStream(httpConn.getOutputStream());
//			dataOutStream.writeBytes(configStr);
//
//			dataOutStream.flush();
//
//			int responseCode = httpConn.getResponseCode();
//			LOG.info(">>>>> createNewConnector responseCode={}",responseCode);
//
//			String readLine = null;
//
//			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
//			StringBuffer response = new StringBuffer();
//			while ((readLine = in.readLine()) != null) {
//				response.append(readLine);
//			}
//			in.close();
//			LOG.info(">>>>> create connenctor response={}",response.toString());
//
//			if (200 == responseCode || 201 == responseCode) {
//				return true;
//			} else {
//				return false;
//			}
//
//		}  finally {
//			if (dataOutStream != null) {
//				try {
//					dataOutStream.flush();
//					dataOutStream.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//			if (httpConn != null )httpConn.disconnect();
//
//		}
//	}
//
//	private Consumer<String, String> createConsumer(String kafkaBootstrapServer, String kafkaGroupId) {
//		Properties props = new Properties();
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
//		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//
//		Consumer<String, String> consumer = new KafkaConsumer<>(props);
//
//		return consumer;
//	}
//	
//	public void shutdownKafkaServer() {
//		LOG.info(">>>>>>>>>>>> shutdownKafkaServer ");
//	
//		LOG.warn(" >>> CALL stopKafka");
//		try {
//			stopKafka();
//		} catch (Exception e) {
//			LOG.error(">>> Error!!!, stopKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//		}
//		
//	}
//	public void shutdownZookeeper() {
//		LOG.info(">>>>>>>>>>>> shutdownZookeeper ");
//	
//		LOG.warn(" >>> CALL stopZookeeper");
//		try {
//			stopZookeeper();
//		} catch (Exception e) {
//			LOG.error(">>> Error!!!, stopZookeeper, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//		}
//		
//	}
//	//	private String getReConfigStr(boolean resetOffset) throws Exception {
//	//		Properties prop = null;
//	//
//	//		try {
//	//			prop = new Properties();
//	//			FileInputStream fis = new FileInputStream(connectConnectorConfigFile);
//	//			prop.load(fis);
//	//		} 
//	//		catch (Exception e){
//	//			LOG.error(">>>>> ErrorMessage={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//	//			throw e;
//	//		}
//	//
//	//		Map<String, Object> map = new HashMap<>();
//	//		Map<String, String> configmap = new HashMap<>();
//	//		for (Entry<Object, Object> e : prop.entrySet()) {
//	//			String key = (String)e.getKey();
//	//			String value =  (String)e.getValue();
//	//			if ("name".equals(key)) {
//	//				map.put(key, value);
//	//			} else {
//	//				if ("reset.offset".equals(key)) {
//	//					value = (resetOffset)? "true" : "false";
//	//				}
//	//				if ("start.scn".equals(key)) {
//	//					value = "";
//	//				}
//	//				configmap.put(key, value);
//	//				//				LOG.info(">>>>>>>>>>>> entry key={}, value={}", (String)e.getKey(), (String)e.getValue());
//	//			}
//	//		}
//	//		map.put("config", configmap);
//	//
//	//		ObjectMapper objectMapper = new ObjectMapper();
//	//		String connectStr = objectMapper.writeValueAsString(map);
//	//
//	//		return connectStr;
//	//	}

