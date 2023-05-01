use arrow2::{
    array::{
        Array, MutableArray, MutableDictionaryArray, MutablePrimitiveArray, MutableUtf8Array,
        TryPush,
    },
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    error::Result,
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::{env, fs::File};

static WRITE_OPTIONS: WriteOptions = WriteOptions {
    write_statistics: true,
    compression: CompressionOptions::Snappy,
    version: Version::V2,
    data_pagesize_limit: Some(65536),
};

static ENCODING_MAP: fn(&DataType) -> Encoding = |data_type: &DataType| -> Encoding {
    use DataType::*;
    match data_type {
        Dictionary(_, _, _) => Encoding::RleDictionary,
        UInt64 => Encoding::DeltaBinaryPacked,
        _ => Encoding::Plain,
    }
};

fn main() {
    let n_rows = env::args()
        .nth(1)
        .unwrap_or("50".to_string())
        .parse::<usize>()
        .unwrap();

    println!("Writing {} rows with arrow2...", n_rows);

    let mut array1: MutableDictionaryArray<i32, MutableUtf8Array<i32>> =
        MutableDictionaryArray::new();
    let mut array2: MutablePrimitiveArray<u64> = MutablePrimitiveArray::new();

    for i in 0..n_rows {
        array1.try_push(Some(format!("foo{}", i % 10))).unwrap();
        array2.push(Some(i as u64));
    }

    let arrays = vec![array1.as_box(), array2.as_box()];

    let mut fields = vec![];

    for (i, array) in arrays.iter().enumerate() {
        let field = Field::new(format!("c{}", i), array.data_type().clone(), true);
        fields.push(field);
    }

    let schema = Schema::from(fields);
    println!("Arrow schema is: {:#?}", schema.clone());

    let chunk = Chunk::new(arrays);

    write_chunk("test.parquet", schema, chunk).unwrap();

    println!(" done");

    println!("Reading {} rows with arrow-rs...", n_rows);
    let file = File::open("test.parquet").unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    println!("Converted arrow schema is: {:#?}", builder.schema());

    let mut reader = builder.build().unwrap();

    while let Some(record_batch) = reader.next() {
        let _record_batch = record_batch.unwrap();
    }

    println!(" done");
}

fn write_chunk(path: &str, schema: Schema, chunk: Chunk<Box<dyn Array>>) -> Result<()> {
    let options = WRITE_OPTIONS;

    let iter = vec![Ok(chunk)];

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, ENCODING_MAP))
        .collect();

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

    // Create a new empty file
    let file = File::create(path)?;

    let mut writer = FileWriter::try_new(file, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;
    Ok(())
}
