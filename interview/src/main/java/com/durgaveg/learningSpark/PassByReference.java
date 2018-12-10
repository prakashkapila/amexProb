package com.durgaveg.learningSpark;

public class PassByReference {
	public void passByValue(String arg)
	{
		arg = arg+ "Something";
		System.out.println("passed by Value\t "+arg);
	}
	public void passByReference(Something som) {
		som.addS("\t Sample Programs ");
		System.out.println(" New val"+som.s);
	}
	public void anotherPass(Double arg)
	{
		arg = arg+ 3;
		System.out.println("passed by Value"+arg);
	}
	public void referencePassing() {
		String arg1 = "Anything";
		Double arg2= new Double(10.5);
		System.out.println("Passing values\t"+arg1+"\t"+arg2);
		anotherPass(arg2);
		passByValue(arg1);
		Something s = new Something();
		s.addS(" Spark Learners");

		System.out.println(" val " +s.s);
		passByReference(s);
	
		System.out.println("Passing values\t"+arg1+"\t"+arg2+" "+s.s);
		

	}
}
