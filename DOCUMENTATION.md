# IFRS9 Pro Backend Documentation

## 1. Data Ingestion Process

The ingestion system is designed for high-volume data processing, using streaming techniques to handle large Excel files efficiently without overloading memory.

### Workflow
1.  **Upload**: User uploads Excel files (Loan Details, Client Data, Guarantees, Collateral).
2.  **Conversion**: Excel files are converted to CSV on-the-fly using `xlsx2csv` to enable stream processing.
3.  **Processing**: `Polars` reads the CSV in batches (chunks) to minimize RAM usage.
4.  **Normalization**: Column headers are normalized (stripped, lowercased, spaces replaced with underscores).
5.  **Database Insertion**: Data is bulk-inserted into PostgreSQL using the `COPY` command for maximum speed.

### Data Ingested & Column Mapping

#### A. Loan Details (`loans` table)
| Excel Header (Normalized) | Database Column | Notes |
| :--- | :--- | :--- |
| `loan_no` | `loan_no` | Unique Identifier |
| `employee_id` | `employee_id` | Foreign Key to Client |
| `loan_amount` | `loan_amount` | Principal |
| `loan_term` | `loan_term` | In Months |
| `monthly_installment` | `monthly_installment` | |
| `accumulated_arrears` | `accumulated_arrears` | |
| `outstanding_loan_balance`| `outstanding_loan_balance`| |
| `loan_issue_date` | `loan_issue_date` | Parsed to YYYY-MM-DD |
| `deduction_start_period` | `deduction_start_period` | Parsed to YYYY-MM-DD |
| `submission_period` | `submission_period` | Parsed to YYYY-MM-DD |
| `maturity_period` | `maturity_period` | Parsed to YYYY-MM-DD |

**Calculated Field during Ingestion:**
*   **NDIA (Number of Days in Arrears)**:
    ```python
    if monthly_installment > 0:
        ndia = (accumulated_arrears / monthly_installment) * 30
    else:
        ndia = 0
    ```

#### B. Client Data (`clients` table)
| Excel Header (Normalized) | Database Column | Notes |
| :--- | :--- | :--- |
| `employee_id` | `employee_id` | Unique Key matching Loan |
| `last_name` (or `lastname`) | `last_name` | |
| `other_names` (or `othernames`)| `other_names` | |
| `residential_address` | `residential_address` | |
| `phone_number` | `phone_number` | |
| `date_of_birth` | `date_of_birth` | Critical for PD calculation |

---

## 2. Staging Process

Staging determines the risk category of a loan based on its **NDIA**. This runs after ingestion.

### A. ECL Staging (IFRS 9)
Classifies loans into **Stage 1, Stage 2, or Stage 3** based on day ranges defined in the Portfolio configuration.

*   **Logic**:
    1.  Recalculates NDIA (JIT consistency check).
    2.  Compares NDIA against configured ranges (e.g., Stage 1: 0-30, Stage 2: 31-90, Stage 3: 90+).
    3.  Updates `loan.ifrs9_stage`.

### B. Local Impairment Staging (BoG / BOG)
Classifies loans into 5 regulatory categories based on configured ranges.

*   **Categories**: `Current`, `OLEM`, `Substandard`, `Doubtful`, `Loss`.
*   **Logic**:
    1.  Recalculates NDIA.
    2.  Compares NDIA against ranges (e.g., Current: 0-30, OLEM: 30-90, etc.).
    3.  Updates `loan.bog_stage`.

---

## 3. Calculators & Methodologies

### A. ECL Calculator (Expected Credit Loss)

The ECL module calculates both **12-month ECL** and **Lifetime ECL** and selects the appropriate one based on the loan's stage.

#### Inputs & Data Used
*   **Loan Data**: Principal, Term, Interest Rate, Monthly Installment, Arrears.
*   **Client Data**: Date of Birth (Age).

#### 1. Probability of Default (PD)
*   **Method**: Machine Learning (Logistic Regression).
*   **Feature**: `Year of Birth` (Age is the primary predictor).
*   **Model Source**: `app/ml_models/logistic_model.pkl`.
*   **Fallback**: Defaults to **5% (0.05)** if the model fails or client age is missing.

#### 2. Loss Given Default (LGD)
*   **Value**: **100% (1.0)**.
*   **Rationale**: Loans are treated as unsecured for this calculation context.

#### 3. Exposure at Default (EAD)
Calculates the theoretical balance of the loan at any future month $t$.

*   **Formula**:
    $$ B_t = P \times \frac{(1+r)^n - (1+r)^t}{(1+r)^n - 1} $$
    $$ EAD = B_t + \text{Accumulated Arrears} $$
    *   $P$: Original Principal
    *   $r$: Monthly Interest Rate
    *   $n$: Total Loan Term (months)
    *   $t$: Months elapsed

#### 4. Calculation Logic
1.  **Generate Schedule**: Creates an amortization schedule for the full loan term.
2.  **Monthly ECL**: For each future month, calculates:
    $$ \text{Marginal ECL} = \text{Balance} \times PD \times LGD $$
3.  **Discounting**: Discounts future ECL values to Present Value (PV) using the Effective Interest Rate (EIR).
4.  **Aggregation**:
    *   **12-Month ECL**: Sum of PV of ECL for the next 12 months.
    *   **Lifetime ECL**: Sum of PV of ECL for the remaining loan term.
5.  **Final Selection**:
    *   **Stage 1** -> 12-Month ECL
    *   **Stage 2 / 3** -> Lifetime ECL

---

### B. Local Impairment Calculator

Calculates the required provision based on local regulatory rules.

#### Inputs
*   **Loan Balance**: `outstanding_loan_balance`
*   **Category Rate**: Provision rate defined in configuration (e.g., Loss = 100%, Current = 1%).

#### Calculation Logic
1.  Group loans by their **BOG Stage** (Current, OLEM, Substandard, Doubtful, Loss).
2.  For each group, sum the `outstanding_loan_balance`.
3.  Apply the provision rate:
    $$ \text{Provision} = \text{Total Balance} \times \text{Rate}(\%) $$
4.  Report Total Provision per category and Grand Total.
