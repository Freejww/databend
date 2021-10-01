// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;

use common_exception::Result;
use common_planners::Expression;
use common_planners::ExpressionVisitor;
use common_planners::Expressions;
use common_planners::Recursion;

pub struct RequireColumnsVisitor {
    pub required_columns: HashSet<String>,
}

impl RequireColumnsVisitor {
    pub fn default() -> Self {
        Self {
            required_columns: HashSet::new(),
        }
    }
}

impl ExpressionVisitor for RequireColumnsVisitor {
    fn pre_visit(self, expr: &Expression) -> Result<Recursion<Self>> {
        match expr {
            Expression::Column(c) => {
                let mut v = self;
                v.required_columns.insert(c.clone());
                Ok(Recursion::Continue(v))
            }
            _ => Ok(Recursion::Continue(self)),
        }
    }
}

pub fn create_scalar_function(op: &str, args: Expressions) -> Expression {
    let op = op.to_string();
    Expression::ScalarFunction { op, args }
}

pub fn create_unary_expression(op: &str, mut args: Expressions) -> Expression {
    let op = op.to_string();
    let expr = Box::new(args.remove(0));
    Expression::UnaryExpression { op, expr }
}

pub fn create_binary_expression(op: &str, mut args: Expressions) -> Expression {
    let op = op.to_string();
    let left = Box::new(args.remove(0));
    let right = Box::new(args.remove(0));
    Expression::BinaryExpression { op, left, right }
}
