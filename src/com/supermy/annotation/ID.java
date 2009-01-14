/**
 * 
 */
package com.supermy.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author my
 * 
 */
//@Target( { ElementType.FIELD, ElementType.TYPE })
@Target( { ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface ID {
	public enum IdType {
		INC, UUID, ParentChild,MD5
	}
	IdType value() default IdType.UUID;
	String md5FieldName() default "name";
}
