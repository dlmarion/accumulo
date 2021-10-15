/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.15.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.compaction.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TRunningCompactions implements org.apache.thrift.TBase<TRunningCompactions, TRunningCompactions._Fields>, java.io.Serializable, Cloneable, Comparable<TRunningCompactions> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TRunningCompactions");

  private static final org.apache.thrift.protocol.TField RUNNING_COMPACTIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("runningCompactions", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TRunningCompactionsStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TRunningCompactionsTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.util.List<TRunningCompaction> runningCompactions; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    RUNNING_COMPACTIONS((short)1, "runningCompactions");

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
        case 1: // RUNNING_COMPACTIONS
          return RUNNING_COMPACTIONS;
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

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.RUNNING_COMPACTIONS, new org.apache.thrift.meta_data.FieldMetaData("runningCompactions", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TRunningCompaction.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TRunningCompactions.class, metaDataMap);
  }

  public TRunningCompactions() {
  }

  public TRunningCompactions(
    java.util.List<TRunningCompaction> runningCompactions)
  {
    this();
    this.runningCompactions = runningCompactions;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TRunningCompactions(TRunningCompactions other) {
    if (other.isSetRunningCompactions()) {
      java.util.List<TRunningCompaction> __this__runningCompactions = new java.util.ArrayList<TRunningCompaction>(other.runningCompactions.size());
      for (TRunningCompaction other_element : other.runningCompactions) {
        __this__runningCompactions.add(new TRunningCompaction(other_element));
      }
      this.runningCompactions = __this__runningCompactions;
    }
  }

  public TRunningCompactions deepCopy() {
    return new TRunningCompactions(this);
  }

  @Override
  public void clear() {
    this.runningCompactions = null;
  }

  public int getRunningCompactionsSize() {
    return (this.runningCompactions == null) ? 0 : this.runningCompactions.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<TRunningCompaction> getRunningCompactionsIterator() {
    return (this.runningCompactions == null) ? null : this.runningCompactions.iterator();
  }

  public void addToRunningCompactions(TRunningCompaction elem) {
    if (this.runningCompactions == null) {
      this.runningCompactions = new java.util.ArrayList<TRunningCompaction>();
    }
    this.runningCompactions.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<TRunningCompaction> getRunningCompactions() {
    return this.runningCompactions;
  }

  public TRunningCompactions setRunningCompactions(@org.apache.thrift.annotation.Nullable java.util.List<TRunningCompaction> runningCompactions) {
    this.runningCompactions = runningCompactions;
    return this;
  }

  public void unsetRunningCompactions() {
    this.runningCompactions = null;
  }

  /** Returns true if field runningCompactions is set (has been assigned a value) and false otherwise */
  public boolean isSetRunningCompactions() {
    return this.runningCompactions != null;
  }

  public void setRunningCompactionsIsSet(boolean value) {
    if (!value) {
      this.runningCompactions = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case RUNNING_COMPACTIONS:
      if (value == null) {
        unsetRunningCompactions();
      } else {
        setRunningCompactions((java.util.List<TRunningCompaction>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case RUNNING_COMPACTIONS:
      return getRunningCompactions();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case RUNNING_COMPACTIONS:
      return isSetRunningCompactions();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TRunningCompactions)
      return this.equals((TRunningCompactions)that);
    return false;
  }

  public boolean equals(TRunningCompactions that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_runningCompactions = true && this.isSetRunningCompactions();
    boolean that_present_runningCompactions = true && that.isSetRunningCompactions();
    if (this_present_runningCompactions || that_present_runningCompactions) {
      if (!(this_present_runningCompactions && that_present_runningCompactions))
        return false;
      if (!this.runningCompactions.equals(that.runningCompactions))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetRunningCompactions()) ? 131071 : 524287);
    if (isSetRunningCompactions())
      hashCode = hashCode * 8191 + runningCompactions.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TRunningCompactions other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetRunningCompactions(), other.isSetRunningCompactions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRunningCompactions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.runningCompactions, other.runningCompactions);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TRunningCompactions(");
    boolean first = true;

    sb.append("runningCompactions:");
    if (this.runningCompactions == null) {
      sb.append("null");
    } else {
      sb.append(this.runningCompactions);
    }
    first = false;
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

  private static class TRunningCompactionsStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TRunningCompactionsStandardScheme getScheme() {
      return new TRunningCompactionsStandardScheme();
    }
  }

  private static class TRunningCompactionsStandardScheme extends org.apache.thrift.scheme.StandardScheme<TRunningCompactions> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TRunningCompactions struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // RUNNING_COMPACTIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list18 = iprot.readListBegin();
                struct.runningCompactions = new java.util.ArrayList<TRunningCompaction>(_list18.size);
                @org.apache.thrift.annotation.Nullable TRunningCompaction _elem19;
                for (int _i20 = 0; _i20 < _list18.size; ++_i20)
                {
                  _elem19 = new TRunningCompaction();
                  _elem19.read(iprot);
                  struct.runningCompactions.add(_elem19);
                }
                iprot.readListEnd();
              }
              struct.setRunningCompactionsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TRunningCompactions struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.runningCompactions != null) {
        oprot.writeFieldBegin(RUNNING_COMPACTIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.runningCompactions.size()));
          for (TRunningCompaction _iter21 : struct.runningCompactions)
          {
            _iter21.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TRunningCompactionsTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TRunningCompactionsTupleScheme getScheme() {
      return new TRunningCompactionsTupleScheme();
    }
  }

  private static class TRunningCompactionsTupleScheme extends org.apache.thrift.scheme.TupleScheme<TRunningCompactions> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TRunningCompactions struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetRunningCompactions()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetRunningCompactions()) {
        {
          oprot.writeI32(struct.runningCompactions.size());
          for (TRunningCompaction _iter22 : struct.runningCompactions)
          {
            _iter22.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TRunningCompactions struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list23 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
          struct.runningCompactions = new java.util.ArrayList<TRunningCompaction>(_list23.size);
          @org.apache.thrift.annotation.Nullable TRunningCompaction _elem24;
          for (int _i25 = 0; _i25 < _list23.size; ++_i25)
          {
            _elem24 = new TRunningCompaction();
            _elem24.read(iprot);
            struct.runningCompactions.add(_elem24);
          }
        }
        struct.setRunningCompactionsIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

