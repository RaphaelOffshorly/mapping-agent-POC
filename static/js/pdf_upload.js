document.addEventListener('DOMContentLoaded', function() {
    // DOM Elements
    const excelModeBtn = document.getElementById('excelModeBtn');
    const pdfModeBtn = document.getElementById('pdfModeBtn');
    const step1Container = document.getElementById('step1Container');
    const step2Container = document.getElementById('step2Container');
    const step3Container = document.getElementById('step3Container');
    const schemaForm = document.getElementById('schemaForm');
    const pdfForm = document.getElementById('pdfForm');
    const excelFile = document.getElementById('excelFile');
    const excelSheetSelectionContainer = document.getElementById('excelSheetSelectionContainer');
    const excelSheetSelect = document.getElementById('excelSheetSelect');
    const schemaEditor = document.getElementById('schemaEditor');
    const saveSchemaBtn = document.getElementById('saveSchemaBtn');
    const generateSchemaBtn = document.getElementById('generateSchemaBtn');
    const extractDataBtn = document.getElementById('extractDataBtn');
    const schemaSpinner = document.getElementById('schemaSpinner');
    const pdfSpinner = document.getElementById('pdfSpinner');
    const errorMessage = document.getElementById('errorMessage');

    // Mode switching
    excelModeBtn.addEventListener('click', function() {
        window.location.href = '/';
    });

    // Handle Excel file selection for schema generation
    excelFile.addEventListener('change', function() {
        if (this.files.length > 0) {
            // Show loading state
            excelSheetSelectionContainer.classList.remove('d-none');
            excelSheetSelect.innerHTML = '<option value="">Loading sheets...</option>';
            
            // Create FormData and append the file
            const formData = new FormData();
            formData.append('file', this.files[0]);
            
            // Send request to get sheets
            fetch('/get_sheets', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    showError(data.error);
                    excelSheetSelectionContainer.classList.add('d-none');
                } else if (data.sheets && data.sheets.length > 0) {
                    // Populate the sheet select dropdown
                    excelSheetSelect.innerHTML = '';
                    data.sheets.forEach(sheet => {
                        const option = document.createElement('option');
                        option.value = sheet;
                        option.textContent = sheet;
                        excelSheetSelect.appendChild(option);
                    });
                    
                    // Show the sheet selection container
                    excelSheetSelectionContainer.classList.remove('d-none');
                } else {
                    excelSheetSelectionContainer.classList.add('d-none');
                }
            })
            .catch(error => {
                showError('An error occurred: ' + error.message);
                excelSheetSelectionContainer.classList.add('d-none');
            });
        } else {
            excelSheetSelectionContainer.classList.add('d-none');
        }
    });

    // Handle schema generation form submission
    schemaForm.addEventListener('submit', function(e) {
        e.preventDefault();
        
        // Hide any previous error
        hideError();
        
        // Show loading spinner
        schemaSpinner.classList.remove('d-none');
        generateSchemaBtn.disabled = true;
        
        // Get form data
        const formData = new FormData(schemaForm);
        
        // Add the selected sheet if available
        if (excelSheetSelect.value) {
            formData.append('excel_sheet_name', excelSheetSelect.value);
        }
        
        // Send request to generate schema
        fetch('/generate_schema', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                showError(data.error);
            } else if (data.schema) {
                // Display the schema in the editor
                schemaEditor.value = JSON.stringify(data.schema, null, 2);
                
                // Move to step 2
                step1Container.classList.add('d-none');
                step2Container.classList.remove('d-none');
            }
        })
        .catch(error => {
            showError('An error occurred: ' + error.message);
        })
        .finally(() => {
            // Hide loading spinner
            schemaSpinner.classList.add('d-none');
            generateSchemaBtn.disabled = false;
        });
    });

    // Handle save schema button
    saveSchemaBtn.addEventListener('click', function() {
        // Validate JSON
        try {
            const schema = JSON.parse(schemaEditor.value);
            
            // Save schema to server
            fetch('/save_schema', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ schema: schema })
            })
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    showError(data.error);
                } else if (data.success) {
                    // Move to step 3
                    step2Container.classList.add('d-none');
                    step3Container.classList.remove('d-none');
                }
            })
            .catch(error => {
                showError('An error occurred: ' + error.message);
            });
        } catch (e) {
            showError('Invalid JSON: ' + e.message);
        }
    });

    // Handle PDF form submission
    pdfForm.addEventListener('submit', function(e) {
        e.preventDefault();
        
        // Hide any previous error
        hideError();
        
        // Show loading spinner
        pdfSpinner.classList.remove('d-none');
        extractDataBtn.disabled = true;
        
        // Get form data
        const formData = new FormData(pdfForm);
        
        // Send request to extract data from PDF
        fetch('/extract_pdf', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                showError(data.error);
            } else if (data.redirect) {
                // Redirect to results page
                window.location.href = data.redirect;
            }
        })
        .catch(error => {
            showError('An error occurred: ' + error.message);
        })
        .finally(() => {
            // Hide loading spinner
            pdfSpinner.classList.add('d-none');
            extractDataBtn.disabled = false;
        });
    });

    // Helper functions
    function showError(message) {
        errorMessage.textContent = message;
        errorMessage.classList.remove('d-none');
    }

    function hideError() {
        errorMessage.classList.add('d-none');
    }
});
