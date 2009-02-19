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


import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;

/**
 * @author my
 * 
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface Column {
	//String name();
	boolean bloomfilter() default false;
	boolean inMemory() default false;
	CompressionType compressionType() default CompressionType.NONE;
	boolean blockCacheEnabled() default false;
	int maxVersions() default 2;
}
