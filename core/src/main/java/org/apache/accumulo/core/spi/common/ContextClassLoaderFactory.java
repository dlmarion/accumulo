package org.apache.accumulo.core.spi.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.classloader.vfs.ReloadingVFSClassLoader;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextClassLoaderFactory {
	
	private static final Logger LOG = LoggerFactory.getLogger(ContextClassLoaderFactory.class);
	
	@Deprecated
  public static final String VFS_CONTEXT_CLASSPATH_PROPERTY = "general.vfs.context.classpath.";
	public static final String CONTEXT_PREFIX = "general.classpath.context.";
	
	private static ContextClassLoaderFactory INSTANCE = null;

	private static final Map<String, ClassLoader> contexts = new HashMap<>();
	
	private static void addContext(String name, ClassLoader contextClassLoader) {
		contexts.put(name, contextClassLoader);
	}
	
	public static ClassLoader getClassLoader(String contextName) {
		if (contexts.containsKey(contextName)) {
			return contexts.get(contextName);
		} else {
			return null;
		}
	}
	
	@Override
	public String toString() {
		return contexts.toString();
	}
	
	public static synchronized void initializeFactory(AccumuloConfiguration conf) throws Exception {
	  if (null == INSTANCE) {
	    var factory = new ContextClassLoaderFactory();
	    var contexts = conf.getAllPropertiesWithPrefixStripped(Property.GENERAL_CLASSPATH_CONTEXT);
	    for (var e : contexts.entrySet()) {
	      LOG.debug("context: {} uses ClassLoader: {}", e.getKey(), e.getValue());
	      try {
	        var loader = Class.forName(e.getValue());
	        if (ClassLoader.class.isAssignableFrom(loader)) {
	          @SuppressWarnings("unchecked")
	          ClassLoader cl = ((Class<? extends ClassLoader>) loader).getDeclaredConstructor().newInstance();
	          addContext(e.getKey(), cl);
	        }
	      } catch (ClassNotFoundException e1) {
	        LOG.error("Unable to load and initialize class: {}. Ensure that the jar containing the ContextClassLoader is on the classpath", e.getValue());
	        throw e1;
	      }
	    }
	    
	    // Handle deprecated VFS context classloader property
	    var vfsContexts = conf.getAllPropertiesWithPrefixStripped(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
      for (var e : vfsContexts.entrySet()) {
        LOG.debug("VFS context: {} uses classpath: {}", e.getKey(), e.getValue());
        String delegation = System.getProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.toString() + e.getKey() + ".delegation", "");
        boolean preDelegation = (delegation.equals("post")) ? false : true;
        ClassLoader cl = new ReloadingVFSClassLoader(Thread.currentThread().getContextClassLoader(), e.getValue(), preDelegation);
        addContext(e.getKey(), cl);
      }
	    INSTANCE = factory;
		} else {
		  LOG.warn("ContextClassLoaderFactory already initialized.");
		}
	}
	
  public static synchronized void removeUnusedContexts(Set<String> configuredContexts) throws IOException {

    Map<String,ClassLoader> unused;

    // ContextManager knows of some set of contexts. This method will be called with
    // the set of currently configured contexts. We will close the contexts that are
    // no longer in the configuration.
      unused = new HashMap<>(contexts);
      unused.keySet().removeAll(configuredContexts);
      contexts.keySet().removeAll(unused.keySet());

    for (Entry<String,ClassLoader> e : unused.entrySet()) {
      if (e.getValue() instanceof Closeable) {
        LOG.info("Closing unused context: {}", e.getKey());
        ((Closeable) e.getValue()).close();
      }
    }
  }

}
