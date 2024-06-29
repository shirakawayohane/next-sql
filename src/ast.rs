#[derive(Debug)]
pub struct Module {
    pub queries: Vec<Query>,
}

#[derive(Debug)]
pub struct QueryBody {
    pub from: FromExpr,
    pub where_clause: Option<Expression>,
    pub select: Vec<Expression>,
}

#[derive(Debug)]
pub struct Target {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug)]
pub struct FromExpr {
    pub target: Target,
    pub joins: Vec<JoinExpr>,
}

#[derive(Debug)]
pub enum JoinType {
    Left,
    Right,
    FullOuter,
    Cross,
}

#[derive(Debug)]
pub struct JoinExpr {
    pub join_type: JoinType,
    pub target: Target,
    pub condition: Expression,
}

#[derive(Debug)]
pub enum Expression {
    Logical {
        left: Box<Expression>,
        op: LogicalOp,
        right: Box<Expression>,
    },
    Equality {
        left: Box<Expression>,
        op: EqualityOp,
        right: Box<Expression>,
    },
    Relational {
        left: Box<Expression>,
        op: RelationalOp,
        right: Box<Expression>,
    },
    Additive {
        left: Box<Expression>,
        op: AdditiveOp,
        right: Box<Expression>,
    },
    Multiplicative {
        left: Box<Expression>,
        op: MultiplicativeOp,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    Primary(PrimaryExpression),
}

#[derive(Debug)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Debug)]
pub enum EqualityOp {
    Equal,
    Unequal,
}

#[derive(Debug)]
pub enum RelationalOp {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Debug)]
pub enum AdditiveOp {
    Add,
    Subtract,
}

#[derive(Debug)]
pub enum MultiplicativeOp {
    Multiply,
    Divide,
    Remainder,
}

#[derive(Debug)]
pub enum UnaryOp {
    Not,
}

#[derive(Debug)]
pub enum PrimaryExpression {
    Column(Column),
    Literal(Literal),
    Variable(Variable),
}

#[derive(Debug)]
pub enum Column {
    Single(String, String),
    WildcardOf(String),
    Wildcard,
}

#[derive(Debug)]
pub enum Literal {
    Numeric(f64),
    String(String),
    Boolean(bool),
}

#[derive(Debug)]
pub struct Variable {
    pub name: String,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum Type {
    BuiltIn(BuiltInType),
}

#[derive(Debug)]
pub struct Argument {
    pub name: String,
    pub typ: Type,
}

#[derive(Debug)]
pub struct QueryDecl {
    pub name: String,
    pub arguments: Vec<Argument>,
}

#[derive(Debug)]
pub struct Query {
    pub decl: QueryDecl,
    pub body: QueryBody,
}
