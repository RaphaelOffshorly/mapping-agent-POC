# CSV Edit Supervisor Test Suite

This test suite validates the functionality of the CSV Edit Supervisor Agent, which handles various Excel-like operations on CSV files.

## Overview

The test suite (`test_csv_edit_supervisor.py`) provides comprehensive testing for:

1. **Basic CRUD Operations**
   - Adding, updating, and deleting rows
   - Adding, updating, and deleting columns

2. **Data Cleaning**
   - Removing duplicate records
   - Handling and removing outliers

3. **Excel-like Operations**
   - Summation and calculations
   - String concatenation and manipulation
   - Conditional updates
   - Statistical calculations
   - Aggregation operations

4. **Advanced Features**
   - Sorting and filtering data
   - Pivot-like summary creation
   - Multiple operations in a single request
   - Verification and retry mechanism

## Running the Tests

### Basic Usage

To run the entire test suite:

```bash
python test_csv_edit_supervisor.py
```

### Advanced Usage with Runner Script

For more flexibility, use the provided runner script:

```bash
python run_supervisor_tests.py [options]
```

#### Options:

- `--test` or `-t`: Run a specific test (e.g., `--test test_1_add_new_row`)
- `--category` or `-c`: Run tests from a specific category:
  - `crud`: CRUD operations tests
  - `cleaning`: Data cleaning tests
  - `excel`: Excel-like operations tests
  - `advanced`: Advanced features tests
- `--list` or `-l`: List all available tests with descriptions
- `--verbose` or `-v`: Increase output verbosity

#### Examples:

```bash
# List all available tests
python run_supervisor_tests.py --list

# Run only CRUD operation tests
python run_supervisor_tests.py --category crud

# Run a single test with verbose output
python run_supervisor_tests.py --test test_9_summation --verbose
```

The test runner will execute the selected tests and provide a summary of passes and failures.

## Test Structure

Each test follows this pattern:
1. Setup a test CSV file with sample data
2. Run the supervisor agent with a specific request
3. Verify that the agent's operations were performed correctly
4. Clean up temporary files

## Test Cases

The test suite includes the following test cases:

1. **CRUD Operations**
   - `test_1_add_new_row`: Tests adding a new row with specific values
   - `test_2_update_row`: Tests updating values in a specific row
   - `test_3_delete_row`: Tests deleting a row based on a condition
   - `test_4_add_column`: Tests adding a new column with specified values
   - `test_5_update_column`: Tests updating values in an entire column
   - `test_6_delete_column`: Tests deleting a column

2. **Data Cleaning**
   - `test_7_remove_duplicates`: Tests removing duplicate rows
   - `test_8_remove_outliers`: Tests handling and removing outliers

3. **Excel Operations**
   - `test_9_summation`: Tests a summation formula (like Excel's SUM)
   - `test_10_concatenation`: Tests string concatenation (like Excel's CONCATENATE)
   - `test_11_conditional_update`: Tests conditional logic (like Excel's IF)
   - `test_16_string_operations`: Tests string manipulation (like Excel's LEFT, RIGHT, UPPER)
   - `test_17_statistical_calculations`: Tests statistical operations (like Excel's AVERAGE, PERCENTILE)
   - `test_18_pivot_like_summary`: Tests creation of summary rows (like Excel's pivot tables)

4. **Advanced Features**
   - `test_12_multiple_operations`: Tests executing multiple operations in one request
   - `test_13_sort_data`: Tests sorting data in the CSV
   - `test_14_filter_data`: Tests filtering rows based on conditions
   - `test_15_aggregate_data`: Tests grouping and aggregating data
   - `test_19_verification_and_retry`: Tests the supervisor's ability to verify changes and retry failed operations

## Verification Utility

The test suite includes utility methods for verification:

- `assert_operation_successful`: Verifies that operations receive a verification PASS
- `print_result_messages`: Prints messages from the agent for debugging

## Sample Data

The test suite creates a test CSV file with sample employee data including:
- First Name, Last Name
- Age, Salary
- Department, Project Hours

This data provides a foundation for testing various operations typical in HR and financial spreadsheets.
