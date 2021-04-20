/**
 *
 */
package com.segmeno.kodo.transport;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author C.Huebert
 *
 */
public interface IKodoEnum{
    @JsonValue
	public String getValue();
}
