use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{plan_err, DFField, DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, Extension, ScalarUDF, TableSource,
};
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias, TableScan,
    UserDefinedLogicalNodeCore, WindowUDF,
};
use datafusion::optimizer::analyzer::{Analyzer, AnalyzerRule};
use datafusion::sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::{dialect::GenericDialect, parser::Parser},
    unparser::plan_to_sql,
    TableReference,
};
use std::{collections::HashMap, fmt, fmt::Debug, sync::Arc};

fn main() {
    let sql = "select id, first_name, last_name, state from customer";

    println!("SQL: {}", sql);
    println!("********");
    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let context_provider = MyContextProvider::new();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = match sql_to_rel.sql_statement_to_plan(statement.clone()) {
        Ok(plan) => plan,
        Err(e) => {
            println!("Error: {:?}", e);
            return;
        }
    };
    println!("Original LogicalPlan:\n {plan:?}");
    println!("********");

    let analyzer = Analyzer::with_rules(vec![
        Arc::new(ModelAnalyzeRule {}),
        Arc::new(ModelGenerationRule {}),
    ]);

    let config = ConfigOptions::default();

    let analyzed = analyzer
        .execute_and_check(&plan, &config, |_, _| {})
        .unwrap();
    println!("Do some modeling:\n {analyzed:?}");
    println!("********");

    // show the planned sql
    let planned = match plan_to_sql(&analyzed) {
        Ok(sql) => sql,
        Err(e) => {
            println!("Error: {:?}", e);
            return;
        }
    };

    println!("unparse to SQL:\n {}", planned);
}

// just follow the example to mock up a ContextProvider
struct MyContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl MyContextProvider {
    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert(
            "customer".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
            ]),
        );
        tables.insert(
            "state".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("sales_tax", DataType::Decimal128(10, 2), false),
            ]),
        );
        tables.insert(
            "orders".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("customer_id", DataType::Int32, false),
                Field::new("item_id", DataType::Int32, false),
                Field::new("quantity", DataType::Int32, false),
                Field::new("price", DataType::Decimal128(10, 2), false),
            ]),
        );
        Self {
            tables,
            options: Default::default(),
        }
    }
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(
        Schema::new_with_metadata(fields, HashMap::new()),
    )))
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(table.clone()),
            _ => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udfs_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udafs_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwfs_names(&self) -> Vec<String> {
        Vec::new()
    }
}

fn analyze_model_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let transformed_data = match plan {
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            ..
        }) => {
            if let LogicalPlan::TableScan(TableScan { table_name, .. }) = input.as_ref() {
                let model = LogicalPlan::Extension(Extension {
                    node: Arc::new(ModelPlanNode::new(table_name.to_string(), expr.clone())),
                });
                let result = Projection::new_from_schema(Arc::new(model), schema.clone());
                Transformed::yes(LogicalPlan::Projection(result))
            } else {
                Transformed::no(LogicalPlan::Projection(Projection::new_from_schema(
                    input, schema,
                )))
            }
        }
        _ => Transformed::no(plan),
    };
    Ok(transformed_data)
}
// Regonzied the model. Turn TableScan from a model to MdoelPlanNode
struct ModelAnalyzeRule {}
impl AnalyzerRule for ModelAnalyzeRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_model_internal).data()
    }

    fn name(&self) -> &str {
        "ModelAnalyzeRule"
    }
}

fn generate_model_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Extension(extension) => {
            if let Some(_) = extension.node.as_any().downcast_ref::<ModelPlanNode>() {
                // Just simulate the model generation. In real world, we should generate the model according to MDL.
                let schema = Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, false),
                    Field::new("state", DataType::Utf8, false),
                ]);
                let table_source = LogicalTableSource::new(SchemaRef::new(schema));
                let projection = None;

                // create a LogicalPlanBuilder for a table scan
                let builder = LogicalPlanBuilder::scan(
                    "remote_customer",
                    Arc::new(table_source),
                    projection,
                )?;
                let result = builder.build().unwrap();
                let alias_name = "customer";
                let alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                    Arc::new(result),
                    alias_name,
                )?);
                Ok(Transformed::yes(alias))
            } else {
                Ok(Transformed::no(LogicalPlan::Extension(extension)))
            }
        }
        _ => Ok(Transformed::no(plan)),
    }
}

// Generate the query plan for the ModelPlanNode
struct ModelGenerationRule {}
impl AnalyzerRule for ModelGenerationRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&generate_model_internal).data()
    }

    fn name(&self) -> &str {
        "ModelGenerationRule"
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct ModelPlanNode {
    model_name: String,
    requried_fields: Vec<Expr>,
    schema_ref: DFSchemaRef,
}

impl ModelPlanNode {
    pub fn new(model_name: String, requried_fields: Vec<Expr>) -> Self {
        let schema = DFSchema::new_with_metadata(
            vec![
                DFField::new(Some("customer"), "id", DataType::Int32, false),
                DFField::new(Some("customer"), "first_name", DataType::Utf8, false),
                DFField::new(Some("customer"), "last_name", DataType::Utf8, false),
                DFField::new(Some("customer"), "state", DataType::Utf8, false),
            ],
            HashMap::new(),
        )
        .unwrap();
    // Just mock a schema here. We should get the schema according to what the plan output.
        let schema_ref = DFSchemaRef::new(schema);
        Self {
            model_name,
            requried_fields,
            schema_ref,
        }
    }
}

// Just mock up the impl for UserDefinedLogicalNodeCore
impl UserDefinedLogicalNodeCore for ModelPlanNode {
    fn name(&self) -> &str {
        "Model"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema_ref
    }

    fn expressions(&self) -> Vec<Expr> {
        self.requried_fields.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Model: name={}", self.model_name)
    }

    fn from_template(&self, _: &[Expr], _: &[LogicalPlan]) -> Self {
        ModelPlanNode::new(self.model_name.clone(), self.requried_fields.clone())
    }
}
