use async_trait::async_trait;
use csv::ReaderBuilder;
use datafusion::arrow::array::{ArrayRef, Int32Builder, RecordBatch};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{ExecutionMode, PlanProperties};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    datasource::{TableProvider, TableType},
    error::Result,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan},
    prelude::*,
};
use std::fs::File;
use std::sync::Arc;
use std::{
    any::Any,
    fmt::{Debug, Formatter},
};

#[derive(Debug, Clone)]
pub struct CustomDataSourceCsv {
    file_path: String,
}

#[async_trait]
impl TableProvider for CustomDataSourceCsv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("projection: {:?}", projection);
        return self.create_physical_plan(projection, self.schema()).await;
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSourceCsv,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl CustomExec {
    fn new(projection: Option<&Vec<usize>>, schema: SchemaRef, db: CustomDataSourceCsv) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            db,
            projection: projection.cloned(),
            properties,
        }
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

impl CustomDataSourceCsv {
    pub(crate) async fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projection, schema, self.clone())))
    }

    pub fn new(path: &str) -> Self {
        Self {
            file_path: path.to_string(),
        }
    }
}

impl ExecutionPlan for CustomExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.db.schema()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let file = File::open(&self.db.file_path).unwrap();
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

        let mut builders: Vec<Box<dyn Any>> = Vec::new();

        for field in self.db.schema().fields() {
            match field.data_type() {
                DataType::Int32 => {
                    let int_builder = Box::new(Int32Builder::new()) as Box<dyn Any>;
                    builders.push(int_builder);
                }
                _ => panic!("Unsupported datatype"),
            }
        }

        for result in reader.records() {
            let record = result.unwrap();
            for (index, field) in self.db.schema().fields().iter().enumerate() {
                match field.data_type() {
                    DataType::Int32 => {
                        if let Some(builder) = builders[index].downcast_mut::<Int32Builder>() {
                            builder.append_value(record[index].parse::<i32>().unwrap());
                        }
                    }
                    _ => panic!("Unsupported datatype"),
                }
            }
        }

        let mut array_vec: Vec<ArrayRef> = Vec::new();

        for (index, field) in self.db.schema().fields().iter().enumerate() {
            match field.data_type() {
                DataType::Int32 => {
                    if let Some(builder) = builders[index].downcast_mut::<Int32Builder>() {
                        array_vec.push(Arc::new(builder.finish()));
                    }
                }
                // Assume all columns are Int32 for now, but imagine we'll need to implement for other types
                _ => panic!("Unsupported datatype"),
            }
            // }
        }
        let projected_batch = RecordBatch::try_new(self.schema(), array_vec)?;

        Ok(Box::pin(MemoryStream::try_new(
            vec![projected_batch],
            self.schema(),
            self.projection.clone(),
        )?))
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
}