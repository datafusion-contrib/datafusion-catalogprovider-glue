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

use datafusion::arrow::array::StringArray;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_catalogprovider_glue::catalog_provider::glue::GlueCatalogProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let mut glue_catalog_provider = GlueCatalogProvider::default().await;

    let databases = vec!["datafusion", "datafusion_testing"];
    for database in databases {
        let register_results = glue_catalog_provider.register_tables(database).await?;
        for result in register_results {
            if result.is_err() {
                // only output tables which were not registered...
                println!("{}", result.err().unwrap());
            }
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
    ctx.sql(&format!("select * from glue.{}.{}", schema, table))
        .await?
        .show_limit(limit)
        .await?;
    Ok(())
}
