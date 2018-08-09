package com.amex.processors;

public class ProcessorBuilder {

	public void startProcessors() {
		AverageSalaryProcessor.startProcessor();
		ERAProcessor.startProcessor();
		PitchingProcessor.startProcessor();
	}
}
