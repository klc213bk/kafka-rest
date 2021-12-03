package com.transglobe.streamingetl.kafka.rest.controller;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.streamingetl.kafka.rest.service.KafkaService;


@RestController
@RequestMapping("/kafka")
public class KafkaController {
	static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

	@Autowired
	private KafkaService kafkaService;
	
	@Autowired
	private ObjectMapper mapper;

	@PostMapping(path="/startZookeeper", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> startZookeeper() {
		logger.info(">>>>controller startZookeeper is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			kafkaService.startZookeeper();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller startZookeeper finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/startKafka", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> startKafka() {
		logger.info(">>>>controller startKafka is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			kafkaService.startKafka();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller startKafka finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/stopZookeeper", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> stopZookeeper() {
		logger.info(">>>>controller stopZookeeper is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			kafkaService.stopZookeeper();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller stopZookeeper finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/stopKafka", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> stopKafka() {
		logger.info(">>>>controller stopKafka is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			kafkaService.stopKafka();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller stopKafka finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/createTopic/{topic}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> createTopic(@PathVariable("topic") String topic) {
		logger.info(">>>>controller createTopic/{} is called", topic);
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			kafkaService.createTopic(topic);
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller createTopic finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/deleteTopic/{topic}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> deleteTopic(@PathVariable("topic") String topic) {
		logger.info(">>>>controller deleteTopic/{} is called", topic);
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			kafkaService.deleteTopic(topic);
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller createTopic finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@GetMapping(path="/zookeeper/alive", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> isZookeeperAlive() {
		logger.info(">>>>controller isZookeeperAlive is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
	
		try {
			boolean isAlive = kafkaService.isZookeeperAlive();
			objectNode.put("returnCode", "0000");
			objectNode.put("alive", isAlive? Boolean.TRUE : Boolean.FALSE);
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller isZookeeperAlive finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@GetMapping(path="/kafka/alive", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> isKafkaAlive() {
		logger.info(">>>>controller isKafkaAlive is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
	
		try {
			boolean isAlive = kafkaService.isKafkaAlive();
			objectNode.put("returnCode", "0000");
			objectNode.put("alive", isAlive? Boolean.TRUE : Boolean.FALSE);
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller isKafkaAlive finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@GetMapping(path="/listTopics", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> listTopics() {
		logger.info(">>>>controller listTopics is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
	
		try {
			List<String> topics = kafkaService.listTopics();
			String jsonStr = writeListToJsonString(topics);
			
			mapper.createArrayNode().add("ggg");
			objectNode.put("returnCode", "0000");
			objectNode.put("topics", jsonStr);
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller listTopics finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	
	public String writeListToJsonString(List<String> list) throws IOException {  
	  
	    final ByteArrayOutputStream out = new ByteArrayOutputStream();
	    final ObjectMapper mapper = new ObjectMapper();

	    mapper.writeValue(out, list);

	    final byte[] data = out.toByteArray();
	    
	    return new String(data);
	    
	}
	//	@PostMapping(path="/startZookeeper", produces=MediaType.APPLICATION_JSON_VALUE)
	//	@ResponseBody
	//	public ResponseEntity<Object> startZookeeper() {
	//		logger.info(">>>>startZookeeper");
	//		long t0 = System.currentTimeMillis();
	//
	//		
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		try {
	//			kafkaService.startZookeeper();
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>startZookeeper finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	//
	//	}
	//	@PostMapping(value="/stopZookeeper")
	//	@ResponseBody
	//	public ResponseEntity<Response> stopZookeeper() {
	//		logger.info(">>>>stopZookeeper");
	//		long t0 = System.currentTimeMillis();
	//
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		try {
	//			kafkaService.stopZookeeper();
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>stopZookeeper finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	//
	//	}

	//
	//	@GetMapping(value="/listTopics")
	//	@ResponseBody
	//	public ResponseEntity<List<String>> listKafkaTopics() {
	//		logger.info(">>>>listTopics");
	//		long t0 = System.currentTimeMillis();
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		List<String> topics = null;
	//		try {
	//			topics = kafkaService.listTopics();
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>listTopics finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(topics);
	//
	//	}
	//
	//	@PostMapping(value="/deleteTopic/{topic}")
	//	@ResponseBody
	//	public ResponseEntity<Response> deleteTopic(@PathVariable("topic") String topic) {
	//		logger.info(">>>>deleteTopic");
	//		long t0 = System.currentTimeMillis();
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		try {
	//			kafkaService.deleteTopic(topic);
	//
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>deleteTopic finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	//
	//	}
	//
	//	// error return
	//	// {"timestamp":"2021-11-04T06:45:05.614+00:00","status":500,"error":"Internal Server Error","message":"","path":"/streamingetl/kafka/createTopic/EBAOPRD1.LS_EBAO.T_ADDRESS"}
	//	@PostMapping(value="/createTopic/{topic}")
	//	@ResponseBody
	//	public ResponseEntity<Response> createTopic(@PathVariable("topic") String topic) {
	//		logger.info(">>>>createTopic");
	//		long t0 = System.currentTimeMillis();
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		try {
	//			kafkaService.createTopic(topic);
	//
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>createTopic finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	//
	//	}
	//
	//	@GetMapping(value="/lastLogminerScn")
	//	@ResponseBody
	//	public ResponseEntity<LastLogminerScn> getKafkaLastLogminerScn(){
	//		logger.info(">>>>getKafkaLastLogminerScn begin");
	//		long t0 = System.currentTimeMillis();
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		Optional<LastLogminerScn> logminerLastScn = null;
	//		try {
	//			logminerLastScn = kafkaService.getKafkaLastLogminerScn();
	//			logger.info("    >>>>getKafkaLastLogminerScn finished.");
	//
	//			if (logminerLastScn.isPresent()) {
	//				return new ResponseEntity<>(logminerLastScn.get(), HttpStatus.OK);
	//			} else {
	//				throw new ResponseStatusException(HttpStatus.NOT_FOUND);
	//			}
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>getKafkaLastLogminerScn finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(logminerLastScn.get());
	//
	//	}
	//
	//
	//	@GetMapping(value="/connectors")
	//	@ResponseBody
	//	public ResponseEntity<List<String>> connectors() {
	//		logger.info(">>>>connectors");
	//		long t0 = System.currentTimeMillis();
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		List<String> list = null;
	//		try {
	//			list = kafkaService.getKafkaconnectors();
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>connectors finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(list);
	//
	//	}
	//
	//	// connector
	//	// oracle-logminer-connector
	//	@GetMapping(value="/connectors/{connector}/status")
	//	@ResponseBody
	//	public ResponseEntity<String> getConnectorStatus(@PathVariable("connector") String connector){
	//		logger.info(">>>>getConnectorStatus");
	//		long t0 = System.currentTimeMillis();
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		String status = null;
	//		try {
	//			status = kafkaService.getConnectorStatus(connector);
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>getConnectorStatus finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(status);
	//
	//	}
	//	@PutMapping(value="/connectors/{connectorName}/pause")
	//	@ResponseBody
	//	public ResponseEntity<Response> pauseConnector(@PathVariable("connectorName") String connectorName) {
	//		logger.info(">>>>pauseConnector");
	//		long t0 = System.currentTimeMillis();
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		try {
	//			kafkaService.pauseConnector(connectorName);
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>pauseConnector finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	//
	//	}
	////	@PostMapping(value="/createConnector/resetOffset")
	////	@ResponseBody
	////	public ResponseEntity<Response> createConnectorResetOffset() {
	////		logger.info(">>>>createConnector w/ResetOffset");
	////		long t0 = System.currentTimeMillis();
	////		String errMsg = null;
	////		String returnCode = "0000";
	////		try {
	////			kafkaService.createNewConnector(true);
	////		} catch (Exception e) {
	////			returnCode = "-9999";
	////			errMsg = ExceptionUtils.getMessage(e);
	////			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	////		}
	////
	////		long t1 = System.currentTimeMillis();
	////
	////		logger.info(">>>>createConnector finished returnCode={}, span={}", returnCode, (t1 - t0));
	////
	////		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	////
	////	}
	////	@PostMapping(value="/createConnector")
	////	@ResponseBody
	////	public ResponseEntity<Response> createConnector() {
	////		logger.info(">>>>createConnector w/o resetOffset");
	////		long t0 = System.currentTimeMillis();
	////		String errMsg = null;
	////		String returnCode = "0000";
	////		try {
	////			kafkaService.createNewConnector(false);
	////		} catch (Exception e) {
	////			returnCode = "-9999";
	////			errMsg = ExceptionUtils.getMessage(e);
	////			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	////		}
	////
	////		long t1 = System.currentTimeMillis();
	////
	////		logger.info(">>>>createConnector finished returnCode={}, span={}", returnCode, (t1 - t0));
	////
	////		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	////
	////	}
	//	@DeleteMapping(value="/connectors/{connectorName}")
	//	@ResponseBody
	//	public ResponseEntity<Response> deleteConnector(@PathVariable("connectorName") String connectorName) {
	//		logger.info(">>>>deleteConnector");
	//		long t0 = System.currentTimeMillis();
	//
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		try {
	//			kafkaService.deleteConnector(connectorName);
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>deleteConnector finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	//
	//	}

	//	@PostMapping(value="/startKafka")
	//	@ResponseBody
	//	public ResponseEntity<Response> startKafka(){
	//		logger.info(">>>>startKafka");
	//		long t0 = System.currentTimeMillis();
	//
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		try {
	//			kafkaService.startKafka();
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>startKafka finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	//
	//
	//	}
	//	@PostMapping(value="/stopKafka")
	//	@ResponseBody
	//	public ResponseEntity<Response> stopKafka() {
	//		logger.info(">>>>stopKafka");
	//		long t0 = System.currentTimeMillis();
	//
	//		String errMsg = null;
	//		String returnCode = "0000";
	//		try {
	//			kafkaService.stopKafka();
	//		} catch (Exception e) {
	//			returnCode = "-9999";
	//			errMsg = ExceptionUtils.getMessage(e);
	//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
	//		}
	//
	//		long t1 = System.currentTimeMillis();
	//
	//		logger.info(">>>>stopKafka finished returnCode={}, span={}", returnCode, (t1 - t0));
	//
	//		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	//
	//
	//	}
}
