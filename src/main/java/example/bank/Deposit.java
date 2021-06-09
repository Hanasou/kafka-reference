/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package example.bank;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class Deposit extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -132871176568002316L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Deposit\",\"namespace\":\"example.bank\",\"fields\":[{\"name\":\"firstName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"amount\",\"type\":\"int\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Deposit> ENCODER =
      new BinaryMessageEncoder<Deposit>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Deposit> DECODER =
      new BinaryMessageDecoder<Deposit>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<Deposit> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<Deposit> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<Deposit> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Deposit>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this Deposit to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a Deposit from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a Deposit instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static Deposit fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.lang.String firstName;
   private int amount;
   private long timestamp;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Deposit() {}

  /**
   * All-args constructor.
   * @param firstName The new value for firstName
   * @param amount The new value for amount
   * @param timestamp The new value for timestamp
   */
  public Deposit(java.lang.String firstName, java.lang.Integer amount, java.lang.Long timestamp) {
    this.firstName = firstName;
    this.amount = amount;
    this.timestamp = timestamp;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return firstName;
    case 1: return amount;
    case 2: return timestamp;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: firstName = value$ != null ? value$.toString() : null; break;
    case 1: amount = (java.lang.Integer)value$; break;
    case 2: timestamp = (java.lang.Long)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'firstName' field.
   * @return The value of the 'firstName' field.
   */
  public java.lang.String getFirstName() {
    return firstName;
  }



  /**
   * Gets the value of the 'amount' field.
   * @return The value of the 'amount' field.
   */
  public int getAmount() {
    return amount;
  }



  /**
   * Gets the value of the 'timestamp' field.
   * @return The value of the 'timestamp' field.
   */
  public long getTimestamp() {
    return timestamp;
  }



  /**
   * Creates a new Deposit RecordBuilder.
   * @return A new Deposit RecordBuilder
   */
  public static example.bank.Deposit.Builder newBuilder() {
    return new example.bank.Deposit.Builder();
  }

  /**
   * Creates a new Deposit RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Deposit RecordBuilder
   */
  public static example.bank.Deposit.Builder newBuilder(example.bank.Deposit.Builder other) {
    if (other == null) {
      return new example.bank.Deposit.Builder();
    } else {
      return new example.bank.Deposit.Builder(other);
    }
  }

  /**
   * Creates a new Deposit RecordBuilder by copying an existing Deposit instance.
   * @param other The existing instance to copy.
   * @return A new Deposit RecordBuilder
   */
  public static example.bank.Deposit.Builder newBuilder(example.bank.Deposit other) {
    if (other == null) {
      return new example.bank.Deposit.Builder();
    } else {
      return new example.bank.Deposit.Builder(other);
    }
  }

  /**
   * RecordBuilder for Deposit instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Deposit>
    implements org.apache.avro.data.RecordBuilder<Deposit> {

    private java.lang.String firstName;
    private int amount;
    private long timestamp;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(example.bank.Deposit.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.firstName)) {
        this.firstName = data().deepCopy(fields()[0].schema(), other.firstName);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.amount)) {
        this.amount = data().deepCopy(fields()[1].schema(), other.amount);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[2].schema(), other.timestamp);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing Deposit instance
     * @param other The existing instance to copy.
     */
    private Builder(example.bank.Deposit other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.firstName)) {
        this.firstName = data().deepCopy(fields()[0].schema(), other.firstName);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.amount)) {
        this.amount = data().deepCopy(fields()[1].schema(), other.amount);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[2].schema(), other.timestamp);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'firstName' field.
      * @return The value.
      */
    public java.lang.String getFirstName() {
      return firstName;
    }


    /**
      * Sets the value of the 'firstName' field.
      * @param value The value of 'firstName'.
      * @return This builder.
      */
    public example.bank.Deposit.Builder setFirstName(java.lang.String value) {
      validate(fields()[0], value);
      this.firstName = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'firstName' field has been set.
      * @return True if the 'firstName' field has been set, false otherwise.
      */
    public boolean hasFirstName() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'firstName' field.
      * @return This builder.
      */
    public example.bank.Deposit.Builder clearFirstName() {
      firstName = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'amount' field.
      * @return The value.
      */
    public int getAmount() {
      return amount;
    }


    /**
      * Sets the value of the 'amount' field.
      * @param value The value of 'amount'.
      * @return This builder.
      */
    public example.bank.Deposit.Builder setAmount(int value) {
      validate(fields()[1], value);
      this.amount = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'amount' field has been set.
      * @return True if the 'amount' field has been set, false otherwise.
      */
    public boolean hasAmount() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'amount' field.
      * @return This builder.
      */
    public example.bank.Deposit.Builder clearAmount() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'timestamp' field.
      * @return The value.
      */
    public long getTimestamp() {
      return timestamp;
    }


    /**
      * Sets the value of the 'timestamp' field.
      * @param value The value of 'timestamp'.
      * @return This builder.
      */
    public example.bank.Deposit.Builder setTimestamp(long value) {
      validate(fields()[2], value);
      this.timestamp = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'timestamp' field has been set.
      * @return True if the 'timestamp' field has been set, false otherwise.
      */
    public boolean hasTimestamp() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'timestamp' field.
      * @return This builder.
      */
    public example.bank.Deposit.Builder clearTimestamp() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Deposit build() {
      try {
        Deposit record = new Deposit();
        record.firstName = fieldSetFlags()[0] ? this.firstName : (java.lang.String) defaultValue(fields()[0]);
        record.amount = fieldSetFlags()[1] ? this.amount : (java.lang.Integer) defaultValue(fields()[1]);
        record.timestamp = fieldSetFlags()[2] ? this.timestamp : (java.lang.Long) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Deposit>
    WRITER$ = (org.apache.avro.io.DatumWriter<Deposit>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Deposit>
    READER$ = (org.apache.avro.io.DatumReader<Deposit>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.firstName);

    out.writeInt(this.amount);

    out.writeLong(this.timestamp);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.firstName = in.readString();

      this.amount = in.readInt();

      this.timestamp = in.readLong();

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.firstName = in.readString();
          break;

        case 1:
          this.amount = in.readInt();
          break;

        case 2:
          this.timestamp = in.readLong();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










