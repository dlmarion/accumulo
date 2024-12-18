/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.process.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class ServerProcessService {

  public interface Iface {

    public void gracefulShutdown() throws org.apache.thrift.TException;

  }

  public interface AsyncIface {

    public void gracefulShutdown(org.apache.thrift.async.AsyncMethodCallback<Void> resultHandler) throws org.apache.thrift.TException;

  }

  public static class Client extends org.apache.thrift.TServiceClient implements Iface {
    public static class Factory implements org.apache.thrift.TServiceClientFactory<Client> {
      public Factory() {}
      @Override
      public Client getClient(org.apache.thrift.protocol.TProtocol prot) {
        return new Client(prot);
      }
      @Override
      public Client getClient(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }

    public Client(org.apache.thrift.protocol.TProtocol prot)
    {
      super(prot, prot);
    }

    public Client(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
      super(iprot, oprot);
    }

    @Override
    public void gracefulShutdown() throws org.apache.thrift.TException
    {
      send_gracefulShutdown();
    }

    public void send_gracefulShutdown() throws org.apache.thrift.TException
    {
      gracefulShutdown_args args = new gracefulShutdown_args();
      sendBaseOneway("gracefulShutdown", args);
    }

  }
  public static class AsyncClient extends org.apache.thrift.async.TAsyncClient implements AsyncIface {
    public static class Factory implements org.apache.thrift.async.TAsyncClientFactory<AsyncClient> {
      private org.apache.thrift.async.TAsyncClientManager clientManager;
      private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
      public Factory(org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
        this.clientManager = clientManager;
        this.protocolFactory = protocolFactory;
      }
    @Override
      public AsyncClient getAsyncClient(org.apache.thrift.transport.TNonblockingTransport transport) {
        return new AsyncClient(protocolFactory, clientManager, transport);
      }
    }

    public AsyncClient(org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.transport.TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
    }

    @Override
    public void gracefulShutdown(org.apache.thrift.async.AsyncMethodCallback<Void> resultHandler) throws org.apache.thrift.TException {
      checkReady();
      gracefulShutdown_call method_call = new gracefulShutdown_call(resultHandler, this, ___protocolFactory, ___transport);
      this.___currentMethod = method_call;
      ___manager.call(method_call);
    }

    public static class gracefulShutdown_call extends org.apache.thrift.async.TAsyncMethodCall<Void> {
      public gracefulShutdown_call(org.apache.thrift.async.AsyncMethodCallback<Void> resultHandler, org.apache.thrift.async.TAsyncClient client, org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.transport.TNonblockingTransport transport) throws org.apache.thrift.TException {
        super(client, protocolFactory, transport, resultHandler, true);
      }

      @Override
      public void write_args(org.apache.thrift.protocol.TProtocol prot) throws org.apache.thrift.TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("gracefulShutdown", org.apache.thrift.protocol.TMessageType.ONEWAY, 0));
        gracefulShutdown_args args = new gracefulShutdown_args();
        args.write(prot);
        prot.writeMessageEnd();
      }

      @Override
      public Void getResult() throws org.apache.thrift.TException {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
          throw new java.lang.IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        return null;
      }
    }

  }

  public static class Processor<I extends Iface> extends org.apache.thrift.TBaseProcessor<I> implements org.apache.thrift.TProcessor {
    private static final org.slf4j.Logger _LOGGER = org.slf4j.LoggerFactory.getLogger(Processor.class.getName());
    public Processor(I iface) {
      super(iface, getProcessMap(new java.util.HashMap<java.lang.String, org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>>()));
    }

    protected Processor(I iface, java.util.Map<java.lang.String, org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends Iface> java.util.Map<java.lang.String,  org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>> getProcessMap(java.util.Map<java.lang.String, org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
      processMap.put("gracefulShutdown", new gracefulShutdown());
      return processMap;
    }

    public static class gracefulShutdown<I extends Iface> extends org.apache.thrift.ProcessFunction<I, gracefulShutdown_args> {
      public gracefulShutdown() {
        super("gracefulShutdown");
      }

      @Override
      public gracefulShutdown_args getEmptyArgsInstance() {
        return new gracefulShutdown_args();
      }

      @Override
      protected boolean isOneway() {
        return true;
      }

      @Override
      protected boolean rethrowUnhandledExceptions() {
        return false;
      }

      @Override
      public org.apache.thrift.TBase getResult(I iface, gracefulShutdown_args args) throws org.apache.thrift.TException {
        iface.gracefulShutdown();
        return null;
      }
    }

  }

  public static class AsyncProcessor<I extends AsyncIface> extends org.apache.thrift.TBaseAsyncProcessor<I> {
    private static final org.slf4j.Logger _LOGGER = org.slf4j.LoggerFactory.getLogger(AsyncProcessor.class.getName());
    public AsyncProcessor(I iface) {
      super(iface, getProcessMap(new java.util.HashMap<java.lang.String, org.apache.thrift.AsyncProcessFunction<I, ? extends org.apache.thrift.TBase, ?>>()));
    }

    protected AsyncProcessor(I iface, java.util.Map<java.lang.String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends AsyncIface> java.util.Map<java.lang.String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase,?>> getProcessMap(java.util.Map<java.lang.String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      processMap.put("gracefulShutdown", new gracefulShutdown());
      return processMap;
    }

    public static class gracefulShutdown<I extends AsyncIface> extends org.apache.thrift.AsyncProcessFunction<I, gracefulShutdown_args, Void> {
      public gracefulShutdown() {
        super("gracefulShutdown");
      }

      @Override
      public gracefulShutdown_args getEmptyArgsInstance() {
        return new gracefulShutdown_args();
      }

      @Override
      public org.apache.thrift.async.AsyncMethodCallback<Void> getResultHandler(final org.apache.thrift.server.AbstractNonblockingServer.AsyncFrameBuffer fb, final int seqid) {
        final org.apache.thrift.AsyncProcessFunction fcall = this;
        return new org.apache.thrift.async.AsyncMethodCallback<Void>() { 
          @Override
          public void onComplete(Void o) {
          }
          @Override
          public void onError(java.lang.Exception e) {
            if (e instanceof org.apache.thrift.transport.TTransportException) {
              _LOGGER.error("TTransportException inside handler", e);
              fb.close();
            } else {
              _LOGGER.error("Exception inside oneway handler", e);
            }
          }
        };
      }

      @Override
      protected boolean isOneway() {
        return true;
      }

      @Override
      public void start(I iface, gracefulShutdown_args args, org.apache.thrift.async.AsyncMethodCallback<Void> resultHandler) throws org.apache.thrift.TException {
        iface.gracefulShutdown(resultHandler);
      }
    }

  }

  @SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
  public static class gracefulShutdown_args implements org.apache.thrift.TBase<gracefulShutdown_args, gracefulShutdown_args._Fields>, java.io.Serializable, Cloneable, Comparable<gracefulShutdown_args>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("gracefulShutdown_args");


    private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new gracefulShutdown_argsStandardSchemeFactory();
    private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new gracefulShutdown_argsTupleSchemeFactory();


    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
;

      private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

      static {
        for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      @org.apache.thrift.annotation.Nullable
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      @org.apache.thrift.annotation.Nullable
      public static _Fields findByName(java.lang.String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final java.lang.String _fieldName;

      _Fields(short thriftId, java.lang.String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      @Override
      public short getThriftFieldId() {
        return _thriftId;
      }

      @Override
      public java.lang.String getFieldName() {
        return _fieldName;
      }
    }
    public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(gracefulShutdown_args.class, metaDataMap);
    }

    public gracefulShutdown_args() {
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public gracefulShutdown_args(gracefulShutdown_args other) {
    }

    @Override
    public gracefulShutdown_args deepCopy() {
      return new gracefulShutdown_args(this);
    }

    @Override
    public void clear() {
    }

    @Override
    public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
      switch (field) {
      }
    }

    @org.apache.thrift.annotation.Nullable
    @Override
    public java.lang.Object getFieldValue(_Fields field) {
      switch (field) {
      }
      throw new java.lang.IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    @Override
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new java.lang.IllegalArgumentException();
      }

      switch (field) {
      }
      throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that) {
      if (that instanceof gracefulShutdown_args)
        return this.equals((gracefulShutdown_args)that);
      return false;
    }

    public boolean equals(gracefulShutdown_args that) {
      if (that == null)
        return false;
      if (this == that)
        return true;

      return true;
    }

    @Override
    public int hashCode() {
      int hashCode = 1;

      return hashCode;
    }

    @Override
    public int compareTo(gracefulShutdown_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      return 0;
    }

    @org.apache.thrift.annotation.Nullable
    @Override
    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      scheme(iprot).read(iprot, this);
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      scheme(oprot).write(oprot, this);
    }

    @Override
    public java.lang.String toString() {
      java.lang.StringBuilder sb = new java.lang.StringBuilder("gracefulShutdown_args(");
      boolean first = true;

      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
      // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private static class gracefulShutdown_argsStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
      @Override
      public gracefulShutdown_argsStandardScheme getScheme() {
        return new gracefulShutdown_argsStandardScheme();
      }
    }

    private static class gracefulShutdown_argsStandardScheme extends org.apache.thrift.scheme.StandardScheme<gracefulShutdown_args> {

      @Override
      public void read(org.apache.thrift.protocol.TProtocol iprot, gracefulShutdown_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            default:
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
          }
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();

        // check for required fields of primitive type, which can't be checked in the validate method
        struct.validate();
      }

      @Override
      public void write(org.apache.thrift.protocol.TProtocol oprot, gracefulShutdown_args struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class gracefulShutdown_argsTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
      @Override
      public gracefulShutdown_argsTupleScheme getScheme() {
        return new gracefulShutdown_argsTupleScheme();
      }
    }

    private static class gracefulShutdown_argsTupleScheme extends org.apache.thrift.scheme.TupleScheme<gracefulShutdown_args> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, gracefulShutdown_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, gracefulShutdown_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      }
    }

    private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
      return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
    }
  }

  private static void unusedMethod() {}
}
