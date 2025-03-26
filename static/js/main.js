document.addEventListener('DOMContentLoaded', function() {
    const uploadForm = document.getElementById('uploadForm');
    const submitBtn = document.getElementById('submitBtn');
    const spinner = document.getElementById('spinner');
    const errorMessage = document.getElementById('errorMessage');
    const fileInput = document.getElementById('file');
    const sheetSelectionContainer = document.getElementById('sheetSelectionContainer');
    const sheetSelect = document.getElementById('sheetSelect');
    const targetColumnsTextarea = document.getElementById('targetColumns');
    
    // Add a file input for target columns
    const targetFileWrapper = document.createElement('div');
    targetFileWrapper.className = 'mb-2';
    targetFileWrapper.innerHTML = `
        <div class="form-check">
            <input class="form-check-input" type="checkbox" id="useTargetFile">
            <label class="form-check-label" for="useTargetFile">
                Use Excel file for target columns
            </label>
        </div>
        <div id="targetFileContainer" class="mt-2 d-none">
            <label for="targetFile" class="form-label">Target Columns Excel File</label>
            <input type="file" class="form-control" id="targetFile" name="target_file" accept=".xlsx,.xls">
            <div id="targetSheetContainer" class="mt-2 d-none">
                <label for="targetSheetSelect" class="form-label">Select Target Sheet</label>
                <select class="form-select" id="targetSheetSelect" name="target_sheet_name">
                    <option value="">Loading sheets...</option>
                </select>
            </div>
        </div>
    `;
    
    // Insert the target file container before the target columns textarea
    targetColumnsTextarea.parentNode.insertBefore(targetFileWrapper, targetColumnsTextarea);
    
    const useTargetFileCheckbox = document.getElementById('useTargetFile');
    const targetFileContainer = document.getElementById('targetFileContainer');
    const targetFileInput = document.getElementById('targetFile');
    const targetSheetContainer = document.getElementById('targetSheetContainer');
    const targetSheetSelect = document.getElementById('targetSheetSelect');
    
    // Handle the checkbox for using Excel file for target columns
    useTargetFileCheckbox.addEventListener('change', function() {
        if (this.checked) {
            targetFileContainer.classList.remove('d-none');
            targetColumnsTextarea.disabled = true;
        } else {
            targetFileContainer.classList.add('d-none');
            targetColumnsTextarea.disabled = false;
            targetSheetContainer.classList.add('d-none');
        }
    });
    
    // Handle file selection for the main Excel file
    fileInput.addEventListener('change', function() {
        if (this.files.length > 0) {
            // Show loading state
            sheetSelectionContainer.classList.remove('d-none');
            sheetSelect.innerHTML = '<option value="">Loading sheets...</option>';
            
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
                    errorMessage.textContent = data.error;
                    errorMessage.classList.remove('d-none');
                    sheetSelectionContainer.classList.add('d-none');
                } else if (data.sheets && data.sheets.length > 0) {
                    // Populate the sheet select dropdown
                    sheetSelect.innerHTML = '';
                    data.sheets.forEach(sheet => {
                        const option = document.createElement('option');
                        option.value = sheet;
                        option.textContent = sheet;
                        sheetSelect.appendChild(option);
                    });
                    
                    // Show the sheet selection container
                    sheetSelectionContainer.classList.remove('d-none');
                } else {
                    sheetSelectionContainer.classList.add('d-none');
                }
            })
            .catch(error => {
                errorMessage.textContent = 'An error occurred: ' + error.message;
                errorMessage.classList.remove('d-none');
                sheetSelectionContainer.classList.add('d-none');
            });
        } else {
            sheetSelectionContainer.classList.add('d-none');
        }
    });
    
    // Handle file selection for the target columns Excel file
    targetFileInput.addEventListener('change', function() {
        if (this.files.length > 0) {
            // Show loading state
            targetSheetContainer.classList.remove('d-none');
            targetSheetSelect.innerHTML = '<option value="">Loading sheets...</option>';
            
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
                    errorMessage.textContent = data.error;
                    errorMessage.classList.remove('d-none');
                    targetSheetContainer.classList.add('d-none');
                } else if (data.sheets && data.sheets.length > 0) {
                    // Populate the sheet select dropdown
                    targetSheetSelect.innerHTML = '';
                    data.sheets.forEach(sheet => {
                        const option = document.createElement('option');
                        option.value = sheet;
                        option.textContent = sheet;
                        targetSheetSelect.appendChild(option);
                    });
                    
                    // Show the sheet selection container
                    targetSheetContainer.classList.remove('d-none');
                    
                    // Load target columns from the selected sheet
                    loadTargetColumnsFromSheet();
                } else {
                    targetSheetContainer.classList.add('d-none');
                }
            })
            .catch(error => {
                errorMessage.textContent = 'An error occurred: ' + error.message;
                errorMessage.classList.remove('d-none');
                targetSheetContainer.classList.add('d-none');
            });
        } else {
            targetSheetContainer.classList.add('d-none');
        }
    });
    
    // Handle target sheet selection change
    targetSheetSelect.addEventListener('change', function() {
        loadTargetColumnsFromSheet();
    });
    
    // Function to load target columns from the selected sheet
    function loadTargetColumnsFromSheet() {
        if (targetFileInput.files.length > 0 && targetSheetSelect.value) {
            // Create FormData
            const formData = new FormData();
            formData.append('file', targetFileInput.files[0]);
            formData.append('sheet', targetSheetSelect.value);
            
            // Send request to get target columns
            fetch('/get_target_columns', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    errorMessage.textContent = data.error;
                    errorMessage.classList.remove('d-none');
                } else if (data.target_columns && data.target_columns.length > 0) {
                    // Update the target columns textarea
                    targetColumnsTextarea.value = data.target_columns.join(', ');
                }
            })
            .catch(error => {
                errorMessage.textContent = 'An error occurred: ' + error.message;
                errorMessage.classList.remove('d-none');
            });
        }
    }
   
    uploadForm.addEventListener('submit', function(e) {
        e.preventDefault();
       
        // Hide any previous error
        errorMessage.classList.add('d-none');
       
        // Show loading spinner
        spinner.classList.remove('d-none');
        submitBtn.disabled = true;
       
        // Get form data
        const formData = new FormData(uploadForm);
        
        // Add the selected sheet if available
        if (sheetSelect.value) {
            formData.append('sheet_name', sheetSelect.value);
        }
        
        // If using target file, add it to the form data
        if (useTargetFileCheckbox.checked && targetFileInput.files.length > 0) {
            formData.append('target_file', targetFileInput.files[0]);
            if (targetSheetSelect.value) {
                formData.append('target_sheet_name', targetSheetSelect.value);
            }
        }
       
        // Send request
        fetch('/upload', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                // Show error
                errorMessage.textContent = data.error;
                errorMessage.classList.remove('d-none');
            } else if (data.redirect) {
                // Redirect to results page
                window.location.href = data.redirect;
            }
        })
        .catch(error => {
            // Show error
            errorMessage.textContent = 'An error occurred: ' + error.message;
            errorMessage.classList.remove('d-none');
        })
        .finally(() => {
            // Hide loading spinner
            spinner.classList.add('d-none');
            submitBtn.disabled = false;
        });
    });
});
