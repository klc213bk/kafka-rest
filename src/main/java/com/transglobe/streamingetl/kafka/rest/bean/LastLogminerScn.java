package com.transglobe.streamingetl.kafka.rest.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LastLogminerScn {

	@JsonProperty("lastScn")
	private Long lastScn;
	
	@JsonProperty("lastCommitScn")
	private Long lastCommitScn;

	public LastLogminerScn() { }
	
	public LastLogminerScn(Long lastScn, Long lastCommitScn) {
		this.lastScn = lastScn;
		this.lastCommitScn = lastCommitScn;
		
	}
	public Long getLastScn() {
		return lastScn;
	}

	public void setLastScn(Long lastScn) {
		this.lastScn = lastScn;
	}

	public Long getLastCommitScn() {
		return lastCommitScn;
	}

	public void setLastCommitScn(Long lastCommitScn) {
		this.lastCommitScn = lastCommitScn;
	}
	
	
	
}
