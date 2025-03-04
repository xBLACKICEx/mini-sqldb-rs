# mini-sqldb-rs 🦀📚

**mini-sqldb-rs** This is a personal learning project where I'm diving into the world of SQL databases by building one from scratch using Rust. The primary goal here is to solidify my understanding of Rust while exploring the inner workings of a database system. (๑•̀ㅂ•́)و✧

> *Yes, the real goal is to write more Rust, explore database internals, wrestle with async & ownership, and maybe—just maybe—make peace with lifetimes. 🧘‍♂️)*

## Architecture 🏗️

To better understand how this database engine works, here are two architecture diagrams:

### High-Level Architecture 🌎
This diagram gives an overview of how the SQL execution pipeline is structured.
<img src="docs/imgs/slqdb-architecture.svg" width="50%"/>

<details>
  <summary>Detailed Architecture ⚙️ (Click to expand)</summary>

<img src="docs/imgs/sqlldb-rs-diagrm_details.svg"/>

</details>

## Project Status 🚧
> *Still at the beginning of the journey! 🌱
This project is my attempt to build a SQL database from scratch, and so far, I’ve only scratched the surface. There's a lot more to come!*

### ✅ Implemented:
- **Database Core**
  - ✅ Database architecture
  - ✅ Basic SQL execution (`SELECT`, `CREATE TABLE`, `INSERT`)
  - ✅ In-memory and basic disk-based storage
  - ✅ Transactions ACID properties and MVCC 

### In Progress 🔨:
- **Next Focus: Refinement Basic sql**
 - 🔜 add update, delete, order by
 - 🔜 limit, offset
 - 🔜 projection
