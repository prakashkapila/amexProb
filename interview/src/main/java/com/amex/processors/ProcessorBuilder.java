package com.amex.processors;

import com.amex.db.DbReader;

public class ProcessorBuilder implements java.io.Serializable{

	
	private static final long serialVersionUID = -5484232270304772650L;

	public void startProcessors() {
		
		AverageSalaryProcessor.startProcessor(new DbReader());
		HallOfFameERAProcessor.startProcessor(new DbReader());
		PitchingProcessor.startProcessor(new DbReader());
		RankingsProcessor.startProcessor(new DbReader());
	}
	
	public static void main(String arg[])
	{
		ProcessorBuilder builder = new ProcessorBuilder();
		builder.startProcessors();
	}
}
