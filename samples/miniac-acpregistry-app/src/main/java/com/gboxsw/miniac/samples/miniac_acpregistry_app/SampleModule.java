package com.gboxsw.miniac.samples.miniac_acpregistry_app;

import com.gboxsw.miniac.*;

/**
 * Sample module as a logical group of data items, variables and subscriptions.
 */
public class SampleModule extends Module {

	/**
	 * Time register.
	 */
	private DataItem<Long> timeRegister;

	@Override
	protected void onInitialize() {
		Application app = getApplication();

		// demo of register data item
		timeRegister = app.getDataItem(Application.DATA_GATEWAY + "/Reader1.System.Time", Long.class);
		app.subscribe(timeRegister.getId(), (message) -> {
			System.out.println("Time register changed: " + timeRegister.getValue());
		});

		// statistical data of the gateway
		app.subscribe("registry/#", (message) -> {
			System.out.println(message.getTopic() + ": " + message.getContent());
		});
	}
}
