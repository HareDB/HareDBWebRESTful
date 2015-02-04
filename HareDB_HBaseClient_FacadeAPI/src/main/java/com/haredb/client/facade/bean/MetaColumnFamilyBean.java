package com.haredb.client.facade.bean;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;

@XmlRootElement
public class MetaColumnFamilyBean {

	private String columnFamilyName;
	private DataBlockEncoding dataBlockEncoding = DataBlockEncoding.NONE;
	private BloomType bloomFilter  = BloomType.NONE;
	private int replicationScope = 0;
	private int versions = 3;
	private Algorithm compression = Algorithm.NONE;
	private int minVersions = 0;
	private int ttl = 2147483647;
	private boolean keepDeletedCells = false;
	private int blocksize = 0;
	private boolean inMemory = false;
	private boolean encodeOnDisk = true;
	private boolean blockcache = true;
	
	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}


	public String getName() {
		return columnFamilyName;
	}
	

	public DataBlockEncoding getDataBlockEncoding() {
		return dataBlockEncoding;
	}

	public void setDataBlockEncoding(DataBlockEncoding dataBlockEncoding) {
		this.dataBlockEncoding = dataBlockEncoding;
	}
	
	public BloomType getBloomFilter() {
		return bloomFilter;
	}

	public void setBloomFilter(BloomType bloomFilter) {
		this.bloomFilter = bloomFilter;
	}

	public int getReplicationScope() {
		return replicationScope;
	}

	public void setReplicationScope(int replicationScope) {
		this.replicationScope = replicationScope;
	}

	public int getVersions() {
		return versions;
	}

	public void setVersions(int versions) {
		this.versions = versions;
	}
	
	public Algorithm getCompression() {
		return compression;
	}

	public void setCompression(Algorithm compression) {
		this.compression = compression;
	}

	public int getMinVersions() {
		return minVersions;
	}

	public void setMinVersions(int minVersions) {
		this.minVersions = minVersions;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}

	public boolean isKeepDeletedCells() {
		return keepDeletedCells;
	}

	public void setKeepDeletedCells(boolean keepDeletedCells) {
		this.keepDeletedCells = keepDeletedCells;
	}
	
	public int getBlocksize() {
		return blocksize;
	}

	public void setBlocksize(int blocksize) {
		this.blocksize = blocksize;
	}

	public boolean isInMemory() {
		return inMemory;
	}

	public void setInMemory(boolean inMemory) {
		this.inMemory = inMemory;
	}

	public boolean isEncodeOnDisk() {
		return encodeOnDisk;
	}

	public void setEncodeOnDisk(boolean encodeOnDisk) {
		this.encodeOnDisk = encodeOnDisk;
	}

	public boolean isBlockcache() {
		return blockcache;
	}

	public void setBlockcache(boolean blockcache) {
		this.blockcache = blockcache;
	}
}
