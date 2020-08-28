package org.apache.accumulo.classloader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;

public class ClassPathPrinter {

  public interface Printer {
    void print(String s);
  }

  public static void printClassPath(ClassLoader cl, boolean debug) {
    printClassPath(cl, System.out::print, debug);
  }

  public static String getClassPath(ClassLoader cl, boolean debug) {
    StringBuilder cp = new StringBuilder();
    printClassPath(cl, cp::append, debug);
    return cp.toString();
  }

  private static void printJar(Printer out, String jarPath, boolean debug, boolean sawFirst) {
    if (debug) {
      out.print("\t");
    }
    if (!debug && sawFirst) {
      out.print(":");
    }
    out.print(jarPath);
    if (debug) {
      out.print("\n");
    }
  }

  public static void printClassPath(ClassLoader cl, Printer out, boolean debug) {
    try {
      ArrayList<ClassLoader> classloaders = new ArrayList<>();

      while (cl != null) {
        classloaders.add(cl);
        cl = cl.getParent();
      }

      Collections.reverse(classloaders);

      int level = 0;

      for (ClassLoader classLoader : classloaders) {

        level++;

        if (debug && level > 1) {
          out.print("\n");
        }
        if (!debug && level < 2) {
          continue;
        }

        String classLoaderDescription;
        switch (level) {
          case 1:
            classLoaderDescription =
                level + ": Bootstrap Classloader";
            break;
          case 2:
            classLoaderDescription =
                level + ": Platform Classloader";
            break;
          case 3:
            classLoaderDescription =
                level + ": System Classloader (loads everything defined by java.class.path and general.classpaths)";
            break;
          case 4:
            classLoaderDescription = level + ": Accumulo Dynamic Classloader "
                + "(loads everything defined by general.dynamic.classpaths)";
            break;
          default:
            classLoaderDescription = level + ": Mystery Classloader ("
                + "someone probably added a classloader and didn't update the switch statement in "
                + ClassPathPrinter.class.getName() + ")";
            break;
        }

        boolean sawFirst = false;
        if (classLoader.getClass().getName().startsWith("jdk.internal")) {
          if (debug) {
            out.print("Level " + classLoaderDescription + " " + classLoader.getClass().getName()
                + " configuration not inspectable.\n");
          }
        } else if (classLoader instanceof URLClassLoader) {
          if (debug) {
            out.print("Level " + classLoaderDescription + " URL classpath items are:\n");
          }
          for (URL u : ((URLClassLoader) classLoader).getURLs()) {
            printJar(out, u.getFile(), debug, sawFirst);
            sawFirst = true;
          }
        } else if (classLoader instanceof VFSClassLoader) {
          if (debug) {
            out.print("Level " + classLoaderDescription + " VFS classpaths items are:\n");
          }
          VFSClassLoader vcl = (VFSClassLoader) classLoader;
          for (FileObject f : vcl.getFileObjects()) {
            printJar(out, f.getURL().getFile(), debug, sawFirst);
            sawFirst = true;
          }
        } else {
          if (debug) {
            out.print("Unknown classloader configuration " + classLoader.getClass() + "\n");
          }
        }
      }
      out.print("\n");
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

}
