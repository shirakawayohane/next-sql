use std::collections::HashMap;
use nextsql_core::ast::*;
use crate::type_mapping::nextsql_type_to_rust;

/// Registry holding valtype mappings from columns to newtype wrappers.
pub(super) struct ValTypeRegistry {
    /// Map from (table, column) -> (valtype name, base type)
    pub(super) column_to_valtype: HashMap<(String, String), (String, BuiltInType)>,
    /// All defined valtypes: name -> base type
    pub(super) valtypes: HashMap<String, BuiltInType>,
}

impl ValTypeRegistry {
    pub(super) fn new() -> Self {
        Self {
            column_to_valtype: HashMap::new(),
            valtypes: HashMap::new(),
        }
    }

    /// Build the registry from all TopLevel items in a module.
    pub(super) fn from_module(module: &Module) -> Self {
        let mut reg = Self::new();

        // First pass: collect all ValType definitions
        for tl in &module.toplevels {
            if let TopLevel::ValType(vt) = tl {
                reg.valtypes.insert(vt.name.clone(), vt.base_type.clone());
                if let Some(ref col_ref) = vt.source_column {
                    let key = (col_ref.table.clone(), col_ref.column.clone());
                    reg.column_to_valtype.entry(key)
                        .or_insert_with(|| (vt.name.clone(), vt.base_type.clone()));
                }
            }
        }

        // Second pass: infer valtypes from Relation join conditions
        for tl in &module.toplevels {
            if let TopLevel::Relation(rel) = tl {
                let pairs = extract_equality_pairs(&rel.join_condition);
                for (left, right) in pairs {
                    let left_key = (left.table.clone(), left.column.clone());
                    let right_key = (right.table.clone(), right.column.clone());

                    if let Some((vt_name, vt_base)) = reg.column_to_valtype.get(&left_key).cloned() {
                        reg.column_to_valtype.entry(right_key).or_insert((vt_name, vt_base));
                    } else if let Some((vt_name, vt_base)) = reg.column_to_valtype.get(&right_key).cloned() {
                        reg.column_to_valtype.entry(left_key).or_insert((vt_name, vt_base));
                    }
                }
            }
        }

        reg
    }

    /// Look up the valtype for a (table, column) pair.
    pub(super) fn lookup(&self, table: &str, column: &str) -> Option<&(String, BuiltInType)> {
        self.column_to_valtype.get(&(table.to_string(), column.to_string()))
    }

    /// Check if a user-defined type name is a known valtype.
    pub(super) fn is_valtype(&self, name: &str) -> bool {
        self.valtypes.contains_key(name)
    }
}

/// Extract equality pairs from a join condition expression.
pub(super) fn extract_equality_pairs(expr: &Expression) -> Vec<(ColumnRef, ColumnRef)> {
    match expr {
        Expression::Binary { left, op: BinaryOp::Equal, right } => {
            if let (Some(left_col), Some(right_col)) = (extract_column_ref(left), extract_column_ref(right)) {
                vec![(left_col, right_col)]
            } else {
                vec![]
            }
        }
        Expression::Binary { left, op: BinaryOp::And, right } => {
            let mut pairs = extract_equality_pairs(left);
            pairs.extend(extract_equality_pairs(right));
            pairs
        }
        _ => vec![]
    }
}

/// Try to extract a ColumnRef from an expression.
pub(super) fn extract_column_ref(expr: &Expression) -> Option<ColumnRef> {
    match expr {
        Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(table, col, _))) => {
            Some(ColumnRef { table: table.clone(), column: col.clone() })
        }
        _ => None
    }
}

/// Generate newtype struct definitions and ToSqlParam impls for all valtypes.
pub(super) fn generate_valtype_structs(out: &mut String, registry: &ValTypeRegistry) {
    // Sort by name for deterministic output
    let mut valtypes: Vec<(&String, &BuiltInType)> = registry.valtypes.iter().collect();
    valtypes.sort_by_key(|(name, _)| (*name).clone());

    for (name, base_type) in valtypes {
        let rust_base = nextsql_type_to_rust(&Type::BuiltIn(base_type.clone()));
        out.push_str(&format!("pub struct {}(pub {});\n\n", name, rust_base));
        out.push_str(&format!("impl nextsql_backend_rust_runtime::ToSqlParam for {} {{\n", name));
        out.push_str("    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {\n");
        out.push_str("        &self.0\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");
    }
}
