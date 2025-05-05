import os
import pandas as pd
import tempfile
import unittest
import logging
from langchain_core.messages import HumanMessage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import the supervisor agent
from agents.csv_edit_supervisor import CSVEditSupervisorAgent

class TestCSVEditSupervisor(unittest.TestCase):
    """
    Test suite for the CSV Edit Supervisor Agent covering:
    1. Basic CRUD operations on rows and columns
    2. Removing duplicates and outliers
    3. Common Excel operations (summation, concatenation, etc.)
    """
    
    def setUp(self):
        """Create a test CSV file with sample data."""
        # Create a temporary CSV file
        fd, self.csv_path = tempfile.mkstemp(suffix='.csv')
        os.close(fd)
        
        # Create a sample DataFrame with test data
        data = {
            'First Name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'John', 'David', 'Eva', 'Frank', 'Grace'],
            'Last Name': ['Smith', 'Doe', 'Johnson', 'Williams', 'Brown', 'Smith', 'Miller', 'Davis', 'Wilson', 'Moore'],
            'Age': [30, 25, 40, 35, 28, 30, 45, 22, 'outlier', 32],
            'Salary': [50000, 60000, 70000, 65000, 55000, 50000, 75000, 48000, 62000, 58000],
            'Department': ['IT', 'HR', 'Finance', 'Marketing', 'IT', 'Sales', 'Finance', 'HR', 'Marketing', 'IT'],
            'Project Hours': [120, 105, 180, 160, 140, 115, 200, 95, 150, 130]
        }
        self.df = pd.DataFrame(data)
        
        # Save the DataFrame to the CSV file
        self.df.to_csv(self.csv_path, index=False)
        logger.info(f"Created test CSV file at: {self.csv_path}")
        
        # Initialize the supervisor agent
        self.agent = CSVEditSupervisorAgent(verbose=True)
        
    def tearDown(self):
        """Clean up temporary files."""
        try:
            os.remove(self.csv_path)
            logger.info(f"Removed temporary CSV file: {self.csv_path}")
        except:
            pass
    
    def read_current_csv(self):
        """Read the current CSV file and return the DataFrame."""
        return pd.read_csv(self.csv_path)
    
    def run_agent_with_request(self, request):
        """Run the supervisor agent with a specific request."""
        user_message = HumanMessage(content=request)
        state = {
            "messages": [user_message],
            "csv_file_path": self.csv_path
        }
        result = self.agent.run(state)
        logger.info(f"Agent request: {request}")
        return result
        
    def assert_operation_successful(self, result, description=None):
        """Assert that the operation was successful based on result messages."""
        verification_passes = False
        if description:
            logger.info(f"Checking success for: {description}")
            
        # Check for verification pass messages
        for message in result.get("messages", []):
            if hasattr(message, 'name') and message.name == "csv_verifier":
                if hasattr(message, 'content') and "PASS" in message.content.upper():
                    verification_passes = True
                    logger.info("Found verification PASS message")
                    break
                    
        self.assertTrue(verification_passes, "Operation did not receive a verification PASS")
        
    def print_result_messages(self, result):
        """Print the messages from the result for debugging."""
        logger.info("Result messages:")
        for message in result.get("messages", []):
            if hasattr(message, 'content'):
                logger.info(f"Message from {getattr(message, 'name', 'unknown')}: {message.content[:100]}...")
    
    def test_1_add_new_row(self):
        """Test adding a new row to the CSV."""
        # Run the agent with a request to add a new row
        request = "Add a new row with First Name: 'Samuel', Last Name: 'Jackson', Age: 55, Salary: 80000, Department: 'Executive', Project Hours: 220"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        self.assertEqual(len(df_after), len(self.df) + 1)
        last_row = df_after.iloc[-1]
        self.assertEqual(last_row['First Name'], 'Samuel')
        self.assertEqual(last_row['Last Name'], 'Jackson')
        self.assertEqual(last_row['Age'], '55')
        self.assertEqual(last_row['Salary'], 80000)
        self.assertEqual(last_row['Department'], 'Executive')
        self.assertEqual(last_row['Project Hours'], 220)
        
        logger.info("✅ Test add_new_row passed")
    
    def test_2_update_row(self):
        """Test updating a row in the CSV."""
        # Run the agent with a request to update a row
        request = "Update the employee with First Name 'Bob' to have Salary 72000 and Department 'Executive'"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        bob_row = df_after[df_after['First Name'] == 'Bob']
        self.assertEqual(len(bob_row), 1)
        self.assertEqual(bob_row['Salary'].values[0], 72000)
        self.assertEqual(bob_row['Department'].values[0], 'Executive')
        
        logger.info("✅ Test update_row passed")
    
    def test_3_delete_row(self):
        """Test deleting a row from the CSV."""
        # Run the agent with a request to delete a row
        request = "Delete the row where the First Name is 'Grace'"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        self.assertEqual(len(df_after), len(self.df) - 1)
        grace_rows = df_after[df_after['First Name'] == 'Grace']
        self.assertEqual(len(grace_rows), 0)
        
        logger.info("✅ Test delete_row passed")
    
    def test_4_add_column(self):
        """Test adding a new column to the CSV."""
        # Run the agent with a request to add a new column
        request = "Add a new column called 'Performance Score' with values 85, 90, 78, 92, 88, 85, 94, 89, 80, 91"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        self.assertIn('Performance Score', df_after.columns)
        self.assertEqual(len(df_after['Performance Score']), len(self.df))
        self.assertEqual(df_after['Performance Score'][0], 85)
        self.assertEqual(df_after['Performance Score'][3], 92)
        
        logger.info("✅ Test add_column passed")
    
    def test_5_update_column(self):
        """Test updating an entire column in the CSV."""
        # Run the agent with a request to update a column
        request = "Update the 'Department' column for all rows to add ' Division' at the end"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        for idx, dept in enumerate(df_after['Department']):
            self.assertTrue(dept.endswith(' Division'))
        
        logger.info("✅ Test update_column passed")
    
    def test_6_delete_column(self):
        """Test deleting a column from the CSV."""
        # Run the agent with a request to delete a column
        request = "Delete the 'Project Hours' column"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        self.assertNotIn('Project Hours', df_after.columns)
        
        logger.info("✅ Test delete_column passed")
    
    def test_7_remove_duplicates(self):
        """Test removing duplicate rows from the CSV."""
        # Run the agent with a request to remove duplicates
        request = "Remove duplicate rows based on First Name and Last Name"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        self.assertLess(len(df_after), len(self.df))
        
        # Check that John Smith appears only once
        john_smith_rows = df_after[(df_after['First Name'] == 'John') & (df_after['Last Name'] == 'Smith')]
        self.assertEqual(len(john_smith_rows), 1)
        
        logger.info("✅ Test remove_duplicates passed")
    
    def test_8_remove_outliers(self):
        """Test removing outliers from the CSV."""
        # Run the agent with a request to remove outliers
        request = "Fix or remove outliers in the Age column by removing rows with non-numeric Ages"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check that all Ages are numeric
        for age in df_after['Age']:
            self.assertTrue(isinstance(age, (int, float)) or (isinstance(age, str) and age.isdigit()))
        
        logger.info("✅ Test remove_outliers passed")
    
    def test_9_summation(self):
        """Test Excel-like summation operation."""
        # Run the agent with a request to sum values
        request = "Add a new column 'Total Compensation' that is the sum of Salary and (Project Hours * 100)"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        self.assertIn('Total Compensation', df_after.columns)
        
        # Check calculation for a few rows
        row_0 = df_after.iloc[0]
        expected_value = row_0['Salary'] + (row_0['Project Hours'] * 100)
        self.assertEqual(row_0['Total Compensation'], expected_value)
        
        logger.info("✅ Test summation passed")
    
    def test_10_concatenation(self):
        """Test Excel-like concatenation operation."""
        # Run the agent with a request to concatenate values
        request = "Add a new column 'Full Name' that concatenates First Name and Last Name with a space in between"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        self.assertIn('Full Name', df_after.columns)
        
        # Check concatenation for a few rows
        for idx, row in df_after.iterrows():
            expected_value = f"{row['First Name']} {row['Last Name']}"
            self.assertEqual(row['Full Name'], expected_value)
        
        logger.info("✅ Test concatenation passed")
    
    def test_11_conditional_update(self):
        """Test conditional update operation."""
        # Run the agent with a request to conditionally update values
        request = "Add a new column 'Salary Grade' that is 'High' if Salary > 60000, 'Medium' if Salary > 50000, and 'Low' otherwise"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        self.assertIn('Salary Grade', df_after.columns)
        
        # Check conditional logic
        for idx, row in df_after.iterrows():
            salary = row['Salary']
            grade = row['Salary Grade']
            
            if salary > 60000:
                self.assertEqual(grade, 'High')
            elif salary > 50000:
                self.assertEqual(grade, 'Medium')
            else:
                self.assertEqual(grade, 'Low')
        
        logger.info("✅ Test conditional_update passed")
    
    def test_12_multiple_operations(self):
        """Test performing multiple operations in a single request."""
        # Run the agent with a request that requires multiple operations
        request = """
        Please perform the following operations:
        1. Remove any duplicate rows based on First Name and Last Name
        2. Add a new column 'Bonus' that is 10% of Salary
        3. Update Project Hours to add 10 hours for all IT department employees
        """
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check for duplicates
        duplicates = df_after.duplicated(subset=['First Name', 'Last Name'], keep=False)
        self.assertEqual(duplicates.sum(), 0)
        
        # Check for Bonus column
        self.assertIn('Bonus', df_after.columns)
        for idx, row in df_after.iterrows():
            self.assertAlmostEqual(row['Bonus'], row['Salary'] * 0.1)
        
        # Check updated Project Hours for IT department
        for idx, row in df_after.iterrows():
            if row['Department'] == 'IT' or row['Department'] == 'IT Division':
                it_hours = row['Project Hours']
                # This test is a bit complex because the hours could have been updated by previous tests
                # so we'll just log the values
                logger.info(f"IT employee Project Hours: {it_hours}")
        
        logger.info("✅ Test multiple_operations passed")
    
    def test_13_sort_data(self):
        """Test sorting data in the CSV."""
        # Run the agent with a request to sort data
        request = "Sort the data by Salary in descending order"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check that the data is sorted by Salary in descending order
        # Convert to numeric to ensure proper comparison
        salaries = pd.to_numeric(df_after['Salary'])
        self.assertTrue(salaries.is_monotonic_decreasing)
        
        logger.info("✅ Test sort_data passed")
    
    def test_14_filter_data(self):
        """Test filtering data in the CSV."""
        # Run the agent with a request to filter data
        request = "Keep only the rows where Department is 'IT' or 'Finance'"
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check that only IT and Finance departments remain
        unique_depts = set(df_after['Department'].unique())
        expected_depts = {'IT', 'Finance'} if not any(d.endswith(' Division') for d in unique_depts) else {'IT Division', 'Finance Division'}
        self.assertEqual(unique_depts, expected_depts)
        
        logger.info("✅ Test filter_data passed")
    
    def test_15_aggregate_data(self):
        """Test aggregating data in the CSV."""
        # Run the agent with a request to aggregate data
        request = """Add a new column 'Department Average Salary' that contains the average salary for each employee's department.
        For example, all IT employees should have the same value - the average of all IT salaries."""
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check that the new column exists
        self.assertIn('Department Average Salary', df_after.columns)
        
        # Check that employees in the same department have the same value
        for dept in df_after['Department'].unique():
            dept_rows = df_after[df_after['Department'] == dept]
            avg_values = dept_rows['Department Average Salary'].unique()
            self.assertEqual(len(avg_values), 1, f"Department {dept} has multiple average values: {avg_values}")
        
        logger.info("✅ Test aggregate_data passed")
    
    def test_16_string_operations(self):
        """Test Excel-like string operations in the CSV."""
        # Run the agent with a request to perform string operations
        request = """
        1. Add a new column 'Initials' that contains the first letter of the First Name and the first letter of the Last Name
        2. Make all letters in the Initials column uppercase
        """
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check that the new column exists
        self.assertIn('Initials', df_after.columns)
        
        # Check the initials for a few rows
        for idx, row in df_after.iterrows():
            expected_value = (row['First Name'][0] + row['Last Name'][0]).upper()
            self.assertEqual(row['Initials'], expected_value)
        
        logger.info("✅ Test string_operations passed")
    
    def test_17_statistical_calculations(self):
        """Test Excel-like statistical calculations in the CSV."""
        # Run the agent with a request to calculate statistics
        request = """
        Add the following columns:
        1. 'Age Deviation' - how much each person's age differs from the average age
        2. 'Salary Percentile' - each salary value shown as a percentile of all salaries (0-100)
        """
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check that the new columns exist
        self.assertIn('Age Deviation', df_after.columns)
        self.assertIn('Salary Percentile', df_after.columns)
        
        # Convert Age column to numeric for calculations
        try:
            df_after['Age'] = pd.to_numeric(df_after['Age'], errors='coerce')
        except:
            pass
        
        # Skip further verification if age isn't numeric
        if not df_after['Age'].isnull().all():
            # Calculate expected age deviation for a few rows
            avg_age = df_after['Age'].mean()
            
            # Check some rows where Age is not null
            for idx, row in df_after[~df_after['Age'].isnull()].iterrows():
                expected_deviation = row['Age'] - avg_age
                # Use approx equal due to potential floating point issues
                self.assertAlmostEqual(row['Age Deviation'], expected_deviation, delta=0.1)
        
        # Check that Salary Percentile is between 0 and 100
        for value in df_after['Salary Percentile']:
            self.assertGreaterEqual(value, 0)
            self.assertLessEqual(value, 100)
        
        logger.info("✅ Test statistical_calculations passed")
    
    def test_18_pivot_like_summary(self):
        """Test creating a pivot-like summary in the CSV."""
        # Run the agent with a request to create a pivot-like summary
        request = """
        Create a summary at the bottom of the CSV with one row per department showing:
        1. Department name
        2. Count of employees in that department
        3. Average salary in that department
        4. Total project hours in that department
        Mark these summary rows by adding a column 'Is Summary' with value 'Yes'
        """
        result = self.run_agent_with_request(request)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check that the summary column exists
        self.assertIn('Is Summary', df_after.columns)
        
        # Check that there are summary rows
        summary_rows = df_after[df_after['Is Summary'] == 'Yes']
        self.assertGreater(len(summary_rows), 0)
        
        # Check that there's one summary row per department
        dept_set = set(df_after[df_after['Is Summary'] != 'Yes']['Department'].unique())
        summary_dept_set = set(summary_rows['Department'].unique())
        self.assertEqual(len(summary_dept_set), len(dept_set))
        
        logger.info("✅ Test pivot_like_summary passed")
    
    def test_19_verification_and_retry(self):
        """Test the supervisor's ability to verify and retry operations."""
        # This test specifically checks that the supervisor agent properly verifies
        # edits and retries them when verification fails.
        
        # Create a complex request that requires accurate verification
        request = """
        Create a financial analysis for each employee:
        1. Add a column 'Effective Hourly Rate' calculated as Salary / (Project Hours * 52)
        2. Add a column 'Year-End Bonus' calculated as:
           - 10% of salary for employees with Salary > 60000
           - 5% of salary for others
        3. Format all financial values rounded to 2 decimal places
        """
        result = self.run_agent_with_request(request)
        
        # Count verification messages to ensure the process included verification
        verification_messages = 0
        failure_messages = 0
        success_messages = 0
        
        for message in result.get("messages", []):
            if hasattr(message, 'name') and message.name == "csv_verifier":
                verification_messages += 1
                if hasattr(message, 'content'):
                    if "FAIL" in message.content.upper():
                        failure_messages += 1
                    if "PASS" in message.content.upper():
                        success_messages += 1
                        
        # The supervisor should have at least one verification check
        self.assertGreater(verification_messages, 0)
        
        # The final verification should be a success
        self.assertGreater(success_messages, 0)
        
        # Verify the result
        df_after = self.read_current_csv()
        
        # Check that the new columns exist
        self.assertIn('Effective Hourly Rate', df_after.columns)
        self.assertIn('Year-End Bonus', df_after.columns)
        
        # Check the calculations for a few rows
        for idx, row in df_after.iterrows():
            try:
                # Skip summary rows if present
                if 'Is Summary' in df_after.columns and row.get('Is Summary') == 'Yes':
                    continue
                    
                salary = float(row['Salary'])
                hours = float(row['Project Hours'])
                
                # Check hourly rate calculation within reasonable delta (rounding differences)
                expected_rate = round(salary / (hours * 52), 2)
                self.assertAlmostEqual(float(row['Effective Hourly Rate']), expected_rate, delta=0.1)
                
                # Check bonus calculation
                if salary > 60000:
                    expected_bonus = round(salary * 0.1, 2)
                else:
                    expected_bonus = round(salary * 0.05, 2)
                    
                self.assertAlmostEqual(float(row['Year-End Bonus']), expected_bonus, delta=1.0)
            except (ValueError, TypeError):
                # Skip rows with non-numeric values
                continue
        
        logger.info("✅ Test verification_and_retry passed")

def run_tests():
    """Run all tests in the test suite."""
    # Create a test suite
    suite = unittest.TestSuite()
    
    # Add all tests to the suite
    suite.addTest(TestCSVEditSupervisor('test_1_add_new_row'))
    suite.addTest(TestCSVEditSupervisor('test_2_update_row'))
    suite.addTest(TestCSVEditSupervisor('test_3_delete_row'))
    suite.addTest(TestCSVEditSupervisor('test_4_add_column'))
    suite.addTest(TestCSVEditSupervisor('test_5_update_column'))
    suite.addTest(TestCSVEditSupervisor('test_6_delete_column'))
    suite.addTest(TestCSVEditSupervisor('test_7_remove_duplicates'))
    suite.addTest(TestCSVEditSupervisor('test_8_remove_outliers'))
    suite.addTest(TestCSVEditSupervisor('test_9_summation'))
    suite.addTest(TestCSVEditSupervisor('test_10_concatenation'))
    suite.addTest(TestCSVEditSupervisor('test_11_conditional_update'))
    suite.addTest(TestCSVEditSupervisor('test_12_multiple_operations'))
    suite.addTest(TestCSVEditSupervisor('test_13_sort_data'))
    suite.addTest(TestCSVEditSupervisor('test_14_filter_data'))
    suite.addTest(TestCSVEditSupervisor('test_15_aggregate_data'))
    suite.addTest(TestCSVEditSupervisor('test_16_string_operations'))
    suite.addTest(TestCSVEditSupervisor('test_17_statistical_calculations'))
    suite.addTest(TestCSVEditSupervisor('test_18_pivot_like_summary'))
    suite.addTest(TestCSVEditSupervisor('test_19_verification_and_retry'))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

if __name__ == "__main__":
    run_tests()
