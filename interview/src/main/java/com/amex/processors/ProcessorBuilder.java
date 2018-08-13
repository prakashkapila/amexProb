package com.amex.processors;

import com.amex.db.DbReader;

public class ProcessorBuilder {

	public void startProcessors() {
		AverageSalaryProcessor.startProcessor();
		ERAProcessor.startProcessor(new DbReader());
		PitchingProcessor.startProcessor(new DbReader());
	}
}
