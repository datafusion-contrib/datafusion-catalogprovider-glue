// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;
use datafusion::arrow::datatypes::{Schema as ArrowSchema};
use datafusion::datasource::file_format::FileFormat;
use datafusion::arrow::array::StringArray;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_catalogprovider_glue::catalog_provider::glue::{
    GlueCatalogProvider, TableRegistrationOptions,
};
use std::sync::Arc;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
use deltalake::DeltaTable;
use async_trait::async_trait;

struct XX {
    table: DeltaTable,
}

#[async_trait]
impl TableProvider for XX {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::new(
            <ArrowSchema as TryFrom<&deltalake::schema::Schema>>::try_from(
                DeltaTable::schema(&self.table).unwrap(),
            )
                .unwrap(),
        )
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(<ArrowSchema as TryFrom<&deltalake::schema::Schema>>::try_from(
            DeltaTable::schema(&self.table).unwrap(),
        )?);
        let filenames = self.table.get_file_uris();

        println!("table uri: {}", &self.table.table_uri);
        for file in self.table.get_file_uris() {
            println!("file: {}", file);
        }

        let df_object_store = Arc::new(S3FileSystem::default().await);

        let partitions = filenames
            .into_iter()
            .zip(self.table.get_active_add_actions())
            .enumerate()
            .map(|(_idx, (fname, action))| {
                let sn = &fname[5..]; // strip s3://
                let sns = sn.to_string();
                // TODO: no way to associate stats per file in datafusion at the moment, see:
                // https://github.com/apache/arrow-datafusion/issues/1301
                Ok(vec![PartitionedFile::new(sns, action.size as u64)])
            })
            .collect::<datafusion::error::Result<_>>()?;

        ParquetFormat::default()
            .create_physical_plan(
                FileScanConfig {
                    object_store: df_object_store,
                    file_schema: schema,
                    file_groups: partitions,
                    statistics: self.table.datafusion_table_statistics(),
                    projection: projection.clone(),
                    limit,
                    table_partition_cols: self.table.get_metadata().unwrap().partition_columns.clone(),
                },
                filters,
            )
            .await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let table_path = "s3://datafusion-delta-testing/COVID-19_NYT";
    let table = deltalake::open_table(table_path).await.unwrap();
    let xx = XX{ table };

    //use deltalake::delta_datafusion;
    ctx.register_table("demo", Arc::new(xx));

    ctx.sql("SELECT * FROM demo")
        .await.unwrap()
        .show_limit(10)
        .await.unwrap();



    Ok(())
}

#[tokio::main]
async fn main2() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let mut glue_catalog_provider = GlueCatalogProvider::default().await;

    let register_results = glue_catalog_provider
        .register_all_with_options(&TableRegistrationOptions::InferSchemaFromData)
        .await?;
    for result in register_results {
        if result.is_err() {
            // only output tables which were not registered...
            println!("{:?}", result);
        }
    }

    ctx.register_catalog("glue", Arc::new(glue_catalog_provider));

    ctx.sql("select * from information_schema.tables")
        .await?
        .show()
        .await?;

    let tables = ctx
        .sql(
            r#"
    select table_schema, table_name
    from information_schema.tables
    where table_catalog='glue'
    and table_schema <> 'information_schema'
    and table_schema <> 'datafusion_delta'
    and table_name <> 'parquet_testing_nullable_impala_parquet'
    and table_name <> 'parquet_testing_nonnullable_impala_parquet'
    and table_name <> 'parquet_testing_nested_lists_snappy_parquet'
    and table_name <> 'parquet_testing_null_list_parquet'
    order by table_catalog asc, table_schema asc, table_name asc
    "#,
        )
        .await?
        .collect()
        .await?;

    for batch in tables {
        let schema_column = batch.column(0);
        let table_column = batch.column(1);
        let schema_array = schema_column
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let table_array = table_column.as_any().downcast_ref::<StringArray>().unwrap();

        for row in 00..batch.num_rows() {
            let schema = schema_array.value(row);
            let table = table_array.value(row);
            match sample(ctx.clone(), schema, table, 10).await {
                Ok(_) => {}
                Err(e) => {
                    println!("failed to sample {}.{} due to {}", schema, table, e)
                }
            };
        }
    }

    Ok(())
}

async fn sample(ctx: SessionContext, schema: &str, table: &str, limit: usize) -> Result<()> {
    println!("sampling glue.{}.{}", schema, table);
    if !table.eq("parquet_testing_encrypt_columns_plaintext_footer_parquet_encrypted")
        && !table.eq("parquet_testing_byte_array_decimal_parquet")
    && !table.starts_with("parquet_testing"){
        ctx.sql(&format!("select * from glue.{}.{}", schema, table))
            .await?
            .show_limit(limit)
            .await?;
    }
    Ok(())
}
