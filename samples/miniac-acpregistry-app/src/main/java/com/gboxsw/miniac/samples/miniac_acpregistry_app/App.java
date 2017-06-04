package com.gboxsw.miniac.samples.miniac_acpregistry_app;

import java.io.File;
import java.util.Scanner;

import com.gbox.miniac.acpregistry.RegistryGateway;
import com.gboxsw.miniac.Application;
import com.gboxsw.miniac.Message;

/**
 * Simple miniac-based application.
 */
public class App {

	public static void main(String[] args) {
		// create application
		Application app = Application.createSimpleApplication();

		// create registry gateway
		File xmlFile = new File("ReaderRS485Gateway.xml");
		RegistryGateway registryGateway = new RegistryGateway(xmlFile);
		app.addGateway("registry", registryGateway);

		// convert all registers to data items
		for (String regName : registryGateway.getAvailableRegisters()) {
			registryGateway.addRegisterToApplication(regName, Application.DATA_GATEWAY, regName);
		}

		// create module and add it to the application
		SampleModule mod = new SampleModule();
		app.addModule(mod);

		// launch the application
		app.launch();

		// wait for enter to exit the application
		try (Scanner s = new Scanner(System.in)) {
			s.nextLine();
		}

		app.publish(new Message("$SYS/exit"));
	}
}
