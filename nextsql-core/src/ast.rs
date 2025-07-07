use std::{collections::HashMap, fmt::Display};

#[derive(Debug, PartialEq)]
pub enum TopLevel {
    Query(Query),
    Mutation(Mutation),
    With(WithStatement),
}

#[derive(Debug, PartialEq)]
pub struct Module {
    pub toplevels: Vec<TopLevel>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Target {
    pub name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FromExpr {
    pub table: String,
    pub joins: Vec<JoinExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinExpr {
    pub join_type: JoinType,
    pub table: String,
    pub condition: Expression,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Binary {
        left: Box<Expression>,
        op: BinaryOp,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    Atomic(AtomicExpression),
}

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOp {
    // 論理演算子
    And,
    Or,
    // 等価演算子
    Equal,
    Unequal,
    // 関係演算子
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    // 加算演算子
    Add,
    Subtract,
    // 乗算演算子
    Multiply,
    Divide,
    Remainder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum UnaryOp {
    Not,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateFunctionType {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregateFunction {
    pub function_type: AggregateFunctionType,
    pub expr: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AtomicExpression {
    Column(Column),
    Literal(Literal),
    Variable(Variable),
    Call(CallExpression),
    IndexAccess(IndexAccess),
    PropertyAccess(PropertyAccess),
    MethodCall(MethodCall),
    SubQuery(Box<SelectStatement>),
    When(WhenExpression),
    Switch(SwitchExpression),
    Aggregate(AggregateFunction),
    Exists(Box<SelectStatement>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Column {
    ImplicitTarget(String),
    ExplicitTarget(String, String),
    WildcardOf(String),
    Wildcard,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Literal {
    Numeric(f64),
    String(String),
    Boolean(bool),
    Null,
    Object(ObjectLiteralExpression),
    Array(ArrayExpression),
}

#[derive(Debug, PartialEq, Clone)]
pub struct CallExpression {
    pub callee: String,
    pub args: Vec<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct IndexAccess {
    pub target: Box<Expression>,
    pub index: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyAccess {
    pub target: Box<Expression>,
    pub property: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MethodCall {
    pub target: Box<Expression>,
    pub method: String,
    pub args: Vec<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ArrayExpression(pub Vec<Expression>);

#[derive(Debug, PartialEq, Clone)]
pub struct Variable {
    pub name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ObjectLiteralExpression(pub HashMap<String, Expression>);

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub enum BuiltInType {
    I16,
    I32,
    I64,
    F32,
    F64,
    Timestamp,
    Timestamptz,
    Date,
    Uuid,
    String,
    Bool,
}

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub struct Insertable(pub Box<Type>);

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub enum UtilityType {
    Insertable(Insertable),
}

impl Display for BuiltInType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                BuiltInType::I16 => "i16",
                BuiltInType::I32 => "i32",
                BuiltInType::I64 => "i64",
                BuiltInType::F32 => "f32",
                BuiltInType::F64 => "f64",
                BuiltInType::Timestamp => "timestamp",
                BuiltInType::Timestamptz => "timestamptz",
                BuiltInType::Date => "date",
                BuiltInType::Uuid => "uuid",
                BuiltInType::String => "string",
                BuiltInType::Bool => "bool",
            }
        )
    }
}

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub enum Type {
    BuiltIn(BuiltInType),
    Utility(UtilityType),
    Optional(Box<Type>),
    Array(Box<Type>),
    UserDefined(String),
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Type::BuiltIn(b) => b.to_string(),
                Type::Optional(t) => format!("{}?", t),
                Type::Utility(u) => {
                    match u {
                        UtilityType::Insertable(t) => format!("Insertable<{}>", t.0),
                    }
                }
                Type::Array(inner) => format!("[{}]", inner),
                Type::UserDefined(name) => name.to_string(),
            }
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct Argument {
    pub name: String,
    pub typ: Type,
}

#[derive(Debug, PartialEq)]
pub struct QueryDecl {
    pub name: String,
    pub arguments: Vec<Argument>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhenExpression {
    pub condition: Box<Expression>,
    pub then_expr: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SwitchCase {
    pub condition: Box<Expression>,
    pub result: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SwitchExpression {
    pub expr: Box<Expression>,
    pub cases: Vec<SwitchCase>,
    pub default: Option<Box<Expression>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectExpression {
    pub expr: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregateExpression {
    pub alias: String,
    pub expr: Expression,
}

#[derive(Debug, PartialEq)]
pub struct WithStatement {
    pub name: String,
    pub body: Vec<QueryStatement>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum QueryClause {
    Where(Expression),
    Select(Vec<SelectExpression>),
    When(WhenClause),
    Limit(Expression),
    OrderBy(Expression),
    Join(JoinExpr),
    GroupBy(Vec<Expression>),
    Aggregate(Vec<AggregateExpression>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhenClause {
    pub condition: Box<Expression>,
    pub clause: Box<QueryClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AliasStatement {
    pub alias: String,
    pub target: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub from: FromExpr,
    pub clauses: Vec<QueryClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum QueryStatement {
    Alias(AliasStatement),
    Select(SelectStatement),
}

#[derive(Debug, PartialEq)]
pub struct QueryBody {
    pub statements: Vec<QueryStatement>,
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub decl: QueryDecl,
    pub body: QueryBody,
}

#[derive(Debug, PartialEq)]
pub struct MutationDecl {
    pub name: String,
    pub arguments: Vec<Argument>,
}

#[derive(Debug, PartialEq)]
pub struct Insert {
    pub into: String,
    pub values: Vec<Expression>,
    pub returning: Option<Vec<Column>>,
}

#[derive(Debug, PartialEq)]
pub struct Update {
    pub target: Target,
    pub where_clause: Option<Expression>,
    pub set: Vec<(String, Expression)>,
    pub returning: Option<Vec<Column>>,
}

#[derive(Debug, PartialEq)]
pub struct Delete {
    pub target: Target,
    pub where_clause: Option<Expression>,
    pub returning: Option<Vec<Column>>,
}

#[derive(Debug, PartialEq)]
pub enum MutationStatement {
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

#[derive(Debug, PartialEq)]
pub enum MutationBodyItem {
    Alias(AliasStatement),
    Mutation(MutationStatement),
}

#[derive(Debug, PartialEq)]
pub struct MutationBody {
    pub items: Vec<MutationBodyItem>,
}

#[derive(Debug, PartialEq)]
pub struct Mutation {
    pub decl: MutationDecl,
    pub body: MutationBody,
}
