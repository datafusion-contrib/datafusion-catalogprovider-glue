// SPDX-License-Identifier: Apache-2.0

//! This modules contains a Parser for Glue Data Types.
use crate::error::GlueError;
use crate::error::Result;

/// Possible Glue data types
/// Known sources:
/// <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html#aws-glue-api-catalog-tables-Column>
/// <https://docs.aws.amazon.com/athena/latest/ug/data-types.html>
#[derive(Debug, PartialEq)]
pub enum GlueDataType {
    TinyInt,
    SmallInt,
    Int,
    Boolean,
    BigInt,
    Float,
    Double,
    Binary,
    Date,
    Timestamp,
    String,
    Char,
    Varchar,
    Decimal(usize, usize),
    Array(Box<GlueDataType>),
    Map(Box<GlueDataType>, Box<GlueDataType>),
    Struct(Vec<GlueField>),
}

/// Represents a Field in a Glue struct.
#[derive(Debug, PartialEq)]
pub struct GlueField {
    pub name: String,
    pub data_type: GlueDataType,
}

/// Parse the string as a glue data type
pub fn parse_glue_data_type(glue_data_type: &str) -> Result<GlueDataType> {
    use pest::Parser;

    // let glue_data_type = glue_data_type.trim();

    let mut pairs = GlueDataTypeParser::parse(Rule::DataType, glue_data_type).map_err(|e| {
        GlueError::GlueDataTypeMapping(format!("Error while parsing {}: {:?}", glue_data_type, e))
    })?;

    let pair = pairs.next().ok_or_else(|| {
        GlueError::GlueDataTypeMapping("Did not find actual type in DataType".to_string())
    })?;

    match pair.as_rule() {
        Rule::TinyInt => Ok(GlueDataType::TinyInt),
        Rule::SmallInt => Ok(GlueDataType::SmallInt),
        Rule::Int => Ok(GlueDataType::Int),
        Rule::Boolean => Ok(GlueDataType::Boolean),
        Rule::BigInt => Ok(GlueDataType::BigInt),
        Rule::Float => Ok(GlueDataType::Float),
        Rule::Double => Ok(GlueDataType::Double),
        Rule::Binary => Ok(GlueDataType::Binary),
        Rule::Timestamp => Ok(GlueDataType::Timestamp),
        Rule::String => Ok(GlueDataType::String),
        Rule::Char => Ok(GlueDataType::Char),
        Rule::Varchar => Ok(GlueDataType::Varchar),
        Rule::Date => Ok(GlueDataType::Date),
        Rule::Decimal => {
            let mut inner = pair.into_inner();
            let precision = inner
                .next()
                .ok_or_else(|| {
                    GlueError::GlueDataTypeMapping(format!(
                        "Did not find precision in {:?}",
                        glue_data_type
                    ))
                })?
                .as_str()
                .parse()
                .map_err(|_| {
                    GlueError::GlueDataTypeMapping(format!(
                        "Failed to parse precision as usize in {:?}",
                        glue_data_type
                    ))
                })?;
            let scale = inner
                .next()
                .ok_or_else(|| {
                    GlueError::GlueDataTypeMapping(format!(
                        "Did not find scale in {:?}",
                        glue_data_type
                    ))
                })?
                .as_str()
                .parse()
                .map_err(|_| {
                    GlueError::GlueDataTypeMapping(format!(
                        "Failed to parse scale as usize in {:?}",
                        glue_data_type
                    ))
                })?;
            Ok(GlueDataType::Decimal(precision, scale))
        }
        Rule::ArrayType => {
            let array_glue_data_type = pair
                .into_inner()
                .next()
                .ok_or_else(|| {
                    GlueError::GlueDataTypeMapping(format!(
                        "Did not find array type in {:?}",
                        glue_data_type
                    ))
                })?
                .as_str();
            let array_arrow_data_type = parse_glue_data_type(array_glue_data_type)?;
            Ok(GlueDataType::Array(Box::new(array_arrow_data_type)))
        }
        Rule::MapType => {
            let mut inner = pair.into_inner();
            let key_glue_data_type = inner
                .next()
                .ok_or_else(|| {
                    GlueError::GlueDataTypeMapping(format!(
                        "Did not key data type in {:?}",
                        glue_data_type
                    ))
                })?
                .as_str();
            let value_glue_data_type = inner
                .next()
                .ok_or_else(|| {
                    GlueError::GlueDataTypeMapping(format!(
                        "Did not find value data type in {:?}",
                        glue_data_type
                    ))
                })?
                .as_str();

            let key_arrow_data_type = parse_glue_data_type(key_glue_data_type)?;
            let value_arrow_data_type = parse_glue_data_type(value_glue_data_type)?;
            Ok(GlueDataType::Map(
                Box::new(key_arrow_data_type),
                Box::new(value_arrow_data_type),
            ))
        }
        Rule::StructType => {
            let inner = pair.into_inner();
            let mut fields = Vec::new();
            for field in inner {
                let mut struct_field_inner = field.into_inner();
                let field_name = struct_field_inner
                    .next()
                    .ok_or_else(|| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Did not find field name in {:?}",
                            glue_data_type
                        ))
                    })?
                    .as_str();
                let field_glue_data_type = struct_field_inner
                    .next()
                    .ok_or_else(|| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Did not find field data type in {:?}",
                            glue_data_type
                        ))
                    })?
                    .as_str();
                let field_arrow_data_type = parse_glue_data_type(field_glue_data_type)?;
                fields.push(GlueField {
                    name: field_name.to_string(),
                    data_type: field_arrow_data_type,
                });
            }
            Ok(GlueDataType::Struct(fields))
        }
        _ => Err(GlueError::NotImplemented(format!(
            "No arrow type for glue_data_type: {}",
            &glue_data_type
        ))),
    }
}

/// `GlueDataTypeParser` implementation for parsing glue datatype strings
#[derive(pest_derive::Parser)]
#[grammar = "glue_data_type_parser/glue_datatype.pest"]
struct GlueDataTypeParser;

//

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::GlueError;
    use crate::error::Result;

    #[test]
    fn test_whitespaces() -> Result<()> {
        let parsed = parse_glue_data_type("struct <ssid:int   , channel_id :string>")?;
        let eq = parsed
            == GlueDataType::Struct(vec![
                GlueField {
                    name: "ssid".to_string(),
                    data_type: GlueDataType::Int,
                },
                GlueField {
                    name: "channel_id".to_string(),
                    data_type: GlueDataType::String,
                },
            ]);
        if !eq {
            return Err(GlueError::GlueDataTypeMapping(format!("parse error")));
        }
        println!("{:?}", parsed);
        Ok(())
    }

    #[test]
    fn test_uppercase() -> Result<()> {
        let parsed = parse_glue_data_type("INTEGER")?;
        let eq = parsed == GlueDataType::Int;
        if !eq {
            Err(GlueError::GlueDataTypeMapping(format!("parse error")))?;
        }
        Ok(())
    }
    #[test]
    fn test_trim() -> Result<()> {
        if !(parse_glue_data_type("  INT ")? == GlueDataType::Int) {
            Err(GlueError::GlueDataTypeMapping(format!("parse error")))?;
        }
        Ok(())
    }
    #[test]
    fn test_case1() -> Result<()> {
        parse_glue_data_type("  ARRAY < STRUCT <evergreen_id: STRING,evergreen_content_offset: BIGINT,ts: BIGINT,duration: BIGINT,network: STRING,ssid: STRING,content_offset: BIGINT>>: GlueDataTypeMapping(\"Error while parsing ARRAY < STRUCT <evergreen_id: STRING,evergreen_content_offset: BIGINT,ts: BIGINT,duration: BIGINT,network: STRING,ssid: STRING,content_offset: BIGINT  > > ")?;
        Ok(())
    }
}
