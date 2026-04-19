use mesh_cypher::*;
fn main() {
    let q = r#"
MATCH ()-[r1]->()-[r2]->()
WITH [r1, r2] AS rs
  LIMIT 1
MATCH (first)-[rs*]->(second)
RETURN first, second
"#;
    match plan(&parse(q).unwrap()) {
        Ok(p) => println!("PLAN OK: {:?}", p),
        Err(e) => println!("ERR: {}", e),
    }
}
