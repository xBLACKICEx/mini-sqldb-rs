# mini-sqldb-rs 🦀📚

**mini-sqldb-rs** This is a personal learning project where I'm diving into the world of SQL databases by building one from scratch using Rust. The primary goal here is to solidify my understanding of Rust while exploring the inner workings of a database system. (๑•̀ㅂ•́)و✧

> *Yes, the real goal is to write more Rust, explore database internals, wrestle with async & ownership, and maybe—just maybe—make peace with lifetimes. 🧘‍♂️)*

## Architecture 🏗️

To better understand how this database engine works, here are two architecture diagrams:

### High-Level Architecture 🌎
This diagram gives an overview of how the SQL execution pipeline is structured.
<img src="https://github.com/user-attachments/assets/317fb9b3-a2af-4a7e-b088-1c35670b0665" width="50%"/>

<details>
  <summary>Detailed Architecture ⚙️ (Click to expand)</summary>

<img src="https://github.com/user-attachments/assets/4deebde0-c2b4-47d8-9063-187e56433001"/>

</details>

## Project Status 🚧
> *Still at the beginning of the journey! 🌱
This project is my attempt to build a SQL database from scratch, and so far, I’ve only scratched the surface. There's a lot more to come!*

### ✅ Completed:
- **Database Core**
  - ✅ Database architecture
  - ✅ SQL Lexer & Parser
  - ✅ Execution Planner
  - ✅ SQL Execution Engine
  - ✅ In-memory storage engine
  - ✅ Basic SQL execution (`SELECT`, `CREATE TABLE`, `INSERT`)

### 🚧 In Progress:
- **Next Focus: Disk-Based Storage Engine**
  - 🔜 **Disk Storage Engine Overview** (LSM Tree, B+ Tree, Bitcask)
  - 🔜 **Basic Disk Storage Implementation**
  - 🔜 **Storage engine startup & cleanup**
