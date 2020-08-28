package org.apache.accumulo.classloader.vfs;

import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.util.Enumeration;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;

/**
 * This class exists to expose methods that are protected in
 * the parent class so that we can use this in a delegate pattern  
 */
public class VFSClassLoaderWrapper extends VFSClassLoader {

  public VFSClassLoaderWrapper(FileObject file, FileSystemManager manager, ClassLoader parent)
      throws FileSystemException {
    super(file, manager, parent);
  }

  public VFSClassLoaderWrapper(FileObject file, FileSystemManager manager)
      throws FileSystemException {
    super(file, manager);
  }

  public VFSClassLoaderWrapper(FileObject[] files, FileSystemManager manager, ClassLoader parent)
      throws FileSystemException {
    super(files, manager, parent);
  }

  public VFSClassLoaderWrapper(FileObject[] files, FileSystemManager manager)
      throws FileSystemException {
    super(files, manager);
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    return super.findClass(name);
  }

  @Override
  public PermissionCollection getPermissions(CodeSource cs) {
    return super.getPermissions(cs);
  }

  @Override
  public URL findResource(String name) {
    return super.findResource(name);
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    return super.findResources(name);
  }

}
