package com.gbox.miniac.acpregistry;

import com.gboxsw.acpmod.registry.Register;
import com.gboxsw.acpmod.registry.Register.ChangeListener;
import com.gboxsw.miniac.Bundle;
import com.gboxsw.miniac.DataItem;

/**
 * Data item that encapsulates a register.
 * 
 * @param <T>
 *            the type of value.
 */
public class RegisterDataItem<T> extends DataItem<T> {

	/**
	 * The source register.
	 */
	private final Register source;

	/**
	 * Constructs the data item that encapsulates a register. Note that updates
	 * of register are not managed by the data item.
	 * 
	 * @param source
	 *            the source register.
	 * @param type
	 *            the type of data item.
	 */
	public RegisterDataItem(Register source, Class<T> type) {
		super(type, source.isReadOnly());
		if (!source.getType().equals(type)) {
			throw new IllegalArgumentException("Type mismatch: type of source register is " + source.getType().getName()
					+ ", but type of constructed data item is " + type.getName());
		}

		this.source = source;
		source.setChangeListener(new ChangeListener() {
			@Override
			public void onChange(Register register) {
				invalidate();
			}
		});
	}
	
	@Override
	protected void onActivate(Bundle savedState) {
		// nothing to do
	}

	@SuppressWarnings("unchecked")
	@Override
	protected T onSynchronizeValue() {
		return (T) source.getValue();
	}

	@Override
	protected void onValueChangeRequested(T newValue) {
		source.setValue(newValue);
	}

	@Override
	protected void onSaveState(Bundle outState) {
		// nothing to do
	}

	@Override
	protected void onDeactivate() {
		// nothing to do
	}
}
