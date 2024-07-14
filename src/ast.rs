use std::{collections::HashMap, fmt::Display};

pub enum ASTNode {
    Module(Module),
    TopLevel(TopLevel),
    Query(Query),
    Mutation(Mutation),
    Target(Target),
    FromExpr(FromExpr),
    JoinExpr(JoinExpr),
    JoinType(JoinType),
    Expression(Expression),
    BinaryOp(BinaryOp),
    UnaryOp(UnaryOp),
    AtomicExpression(AtomicExpression),
    Column(Column),
    Literal(Literal),
    CallExpression(CallExpression),
    IndexAccess(IndexAccess),
    ArrayExpression(ArrayExpression),
    Variable(Variable),
    ObjectLiteralExpression(ObjectLiteralExpression),
    BuiltInType(BuiltInType),
    Insertable(Insertable),
    UtilityType(UtilityType),
    Type(Type),
    Argument(Argument),
    QueryDecl(QueryDecl),
    QueryBody(QueryBody),
    MutationDecl(MutationDecl),
    Insert(Insert),
    Update(Update),
    MutationBody(MutationBody),
}

#[derive(Debug, PartialEq)]
pub enum TopLevel {
    Query(Query),
    Mutation(Mutation),
}

#[derive(Debug, PartialEq)]
pub struct Module {
    pub toplevels: Vec<TopLevel>,
}

#[derive(Debug, PartialEq)]
pub struct Target {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct FromExpr {
    pub target: Target,
    pub joins: Vec<JoinExpr>,
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

#[derive(Debug, PartialEq)]
pub struct JoinExpr {
    pub join_type: JoinType,
    pub target: Target,
    pub condition: Expression,
}

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum UnaryOp {
    Not,
}

#[derive(Debug, PartialEq)]
pub enum AtomicExpression {
    Column(Column),
    Literal(Literal),
    Variable(Variable),
    Call(CallExpression),
    IndexAccess(IndexAccess),
}

#[derive(Debug, PartialEq)]
pub enum Column {
    ImplicitTarget(String),
    ExplicitTarget(String, String),
    WildcardOf(String),
    Wildcard,
}

#[derive(Debug, PartialEq)]
pub enum Literal {
    Numeric(f64),
    String(String),
    Boolean(bool),
    Object(ObjectLiteralExpression),
    Array(ArrayExpression),
}

#[derive(Debug, PartialEq)]
pub struct CallExpression {
    pub callee: String,
    pub args: Vec<Expression>,
}

#[derive(Debug, PartialEq)]
pub struct IndexAccess {
    pub target: Box<Expression>,
    pub index: Box<Expression>,
}

#[derive(Debug, PartialEq)]
pub struct ArrayExpression(pub Vec<Expression>);

#[derive(Debug, PartialEq)]
pub struct Variable {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct ObjectLiteralExpression(pub HashMap<String, Expression>);

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub struct Insertable(pub Box<Type>);

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub struct QueryBody {
    pub from: FromExpr,
    pub where_clause: Option<Expression>,
    pub select: Vec<Expression>,
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
pub enum MutationBody {
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

#[derive(Debug, PartialEq)]
pub struct Mutation {
    pub decl: MutationDecl,
    pub body: MutationBody,
}
