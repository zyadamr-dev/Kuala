# 🐨 Kuala

**Kuala** — A simple, Go-powered data analysis library inspired by **pandas!** Bring the power of data frames to your Go projects with minimal boilerplate.

---

## 📋 Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

---

## ✨ Features

- **CSV I/O**: Read and manipulate CSV files with ease.
- **Data Operations**: Perform common analyses:
  - `Sum()`
  - `Std()` (Standard Deviation)
  - `Col()` specific columns
  - `Loc()` to retrieve specific rows
- **Lightweight & Fast**: Designed for high performance in Go environments.

---

## 📦 Installation

```bash
go get github.com/zyadamr-dev/Kuala
```

---

## 🚀 Usage

```go
package main

import (
    "fmt"
    "github.com/zyadamr-dev/Kuala/io"
)

func main() {
    // Load the CSV data
    df, err := io.ReadCSV("data.csv")
    if err != nil {
        panic(err)
    }

    // Sum of the "Age" column
    totalAge := df.Sum("Age")
    fmt.Println("Total Age:", totalAge)

    // Standard deviation of the "Score" column
    scoreStd := df.Std("Score")
    fmt.Println("Score Std Dev:", scoreStd)

    // Select "Name" and "Email" columns
    subset := df.Select("Name", "Email")
    fmt.Println(subset)

    // Retrieve the 5th row (zero-based index)
    row := df.Row(4)
    fmt.Println("Row 5:", row)
}
```

---

## 📄 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.
