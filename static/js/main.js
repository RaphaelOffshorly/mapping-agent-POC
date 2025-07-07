document.addEventListener('DOMContentLoaded', function() {
    // Schema Builder variables
    let currentSchema = null;
    const schemaModal = new bootstrap.Modal(document.getElementById('schemaBuilderModal'));
    const useSchemaBuilderCheckbox = document.getElementById('useSchemaBuilder');
    const uploadForm = document.getElementById('uploadForm');
    const submitBtn = document.getElementById('submitBtn');
    const spinner = document.getElementById('spinner');
    const errorMessage = document.getElementById('errorMessage');
    const fileInput = document.getElementById('file');
    const sheetSelectionContainer = document.getElementById('sheetSelectionContainer');
    const sheetSelect = document.getElementById('sheetSelect');
    const targetColumnsTextarea = document.getElementById('targetColumns');
    const pdfModeBtn = document.getElementById('pdfModeBtn');
    
    // Mode switching
    pdfModeBtn.addEventListener('click', function() {
        window.location.href = '/pdf_upload';
    });
    
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
        
        // Check if schema builder is enabled
        if (useSchemaBuilderCheckbox.checked) {
            openSchemaBuilder();
            return;
        }
       
        // Regular submission without schema builder
        submitForm();
    });
    
    // Function to submit the form
    function submitForm(schema = null) {
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
        
        // Add schema if provided
        if (schema) {
            formData.append('schema', JSON.stringify(schema));
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
    }
    
    // Schema Builder Functions
    
    // Function to open schema builder modal and generate initial schema
    function openSchemaBuilder() {
        // Get target columns
        let targetColumnsInput = targetColumnsTextarea.value;
        let targetColumns = [];
        
            // Gather target columns from textarea or file
            if (useTargetFileCheckbox.checked && targetFileInput.files.length > 0) {
                // We'll use the file-based target columns
                console.log("Using target file for schema builder");
                
                // Show loading state in modal
                document.getElementById('schemaTableBody').innerHTML = '<tr><td colspan="4" class="text-center">Loading schema data from target file...</td></tr>';
                schemaModal.show();
                
                const formData = new FormData();
                formData.append('target_file', targetFileInput.files[0]);  // Fixed: Use 'target_file' not 'file'
                
                if (targetSheetSelect.value) {
                    formData.append('target_sheet_name', targetSheetSelect.value);
                }
                
                // Debug log formData contents
                console.log("Target file name:", targetFileInput.files[0].name);
                console.log("Target sheet:", targetSheetSelect.value);
            
            createSchema(formData);
        } else if (targetColumnsInput.trim()) {
            // Parse comma-separated target columns
            targetColumns = targetColumnsInput.split(',').map(col => col.trim()).filter(col => col);
            
            if (targetColumns.length === 0) {
                errorMessage.textContent = 'Please enter target columns';
                errorMessage.classList.remove('d-none');
                return;
            }
            
            // Show loading state in modal
            document.getElementById('schemaTableBody').innerHTML = '<tr><td colspan="4" class="text-center">Generating schema...</td></tr>';
            schemaModal.show();
            
            // Prepare form data
            const formData = new FormData();
            formData.append('target_columns', targetColumnsInput);
            
            // Do NOT add the source file here
            // We want to create schema based solely on the target columns list
            // when no target file is provided
            
            createSchema(formData);
        } else {
            errorMessage.textContent = 'Please enter target columns or select a target file';
            errorMessage.classList.remove('d-none');
            return;
        }
        
        // Load saved schemas list
        loadSavedSchemas();
    }
    
    // Function to create a schema
    function createSchema(formData) {
        // Call API to create schema
        fetch('/create_schema', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                document.getElementById('schemaTableBody').innerHTML = 
                    `<tr><td colspan="4" class="text-danger">${data.error}</td></tr>`;
                return;
            }
            
            // Store the schema
            currentSchema = data.schema;
            
            // Render schema table
            renderSchemaTable(currentSchema);
        })
        .catch(error => {
            document.getElementById('schemaTableBody').innerHTML = 
                `<tr><td colspan="4" class="text-danger">Error: ${error.message}</td></tr>`;
        });
    }
    
    // Function to render the schema table
    function renderSchemaTable(schema) {
        const tableBody = document.getElementById('schemaTableBody');
        tableBody.innerHTML = '';
        
        // For each property in the schema
        Object.entries(schema.properties).forEach(([columnName, columnSchema]) => {
            const row = document.createElement('tr');
            
            // Column name (not editable)
            const nameCell = document.createElement('td');
            nameCell.textContent = columnName;
            nameCell.className = 'fw-bold';
            
            // Data type selector with support for array types
            const typeCell = document.createElement('td');
            const typeContainer = document.createElement('div');
            
            // Main type selector
            const typeSelect = document.createElement('select');
            typeSelect.className = 'form-select mb-2';
            typeSelect.dataset.column = columnName;
            
            // Determine the actual type (could be in anyOf structure)
            let actualType = columnSchema.type;
            let isArrayType = false;
            let arrayItemType = 'string';
            
            // Check if this is an anyOf structure with an array
            if (columnSchema.anyOf) {
                const arrayItem = columnSchema.anyOf.find(item => item.type === 'array');
                if (arrayItem) {
                    actualType = 'array';
                    isArrayType = true;
                    if (arrayItem.items && arrayItem.items.type) {
                        arrayItemType = arrayItem.items.type;
                    }
                } else {
                    // Find the non-null type
                    const nonNullItem = columnSchema.anyOf.find(item => item.type !== 'null');
                    if (nonNullItem) {
                        actualType = nonNullItem.type;
                    }
                }
            } else if (columnSchema.type === 'array') {
                isArrayType = true;
                if (columnSchema.items && columnSchema.items.type) {
                    arrayItemType = columnSchema.items.type;
                }
            }
            
            const basicTypes = ['string', 'number', 'boolean', 'array'];
            basicTypes.forEach(type => {
                const option = document.createElement('option');
                option.value = type;
                option.textContent = type.charAt(0).toUpperCase() + type.slice(1);
                option.selected = actualType === type;
                typeSelect.appendChild(option);
            });
            
            // Array item type selector (only shown for arrays)
            const arrayItemContainer = document.createElement('div');
            arrayItemContainer.className = 'mt-2';
            arrayItemContainer.style.display = isArrayType ? 'block' : 'none';
            
            const arrayItemLabel = document.createElement('label');
            arrayItemLabel.textContent = 'Array Item Type:';
            arrayItemLabel.className = 'form-label small';
            
            const arrayItemSelect = document.createElement('select');
            arrayItemSelect.className = 'form-select form-select-sm';
            
            const itemTypes = ['string', 'number', 'boolean'];
            itemTypes.forEach(itemType => {
                const option = document.createElement('option');
                option.value = itemType;
                option.textContent = itemType.charAt(0).toUpperCase() + itemType.slice(1);
                
                // Set selected based on the detected arrayItemType
                option.selected = arrayItemType === itemType;
                
                arrayItemSelect.appendChild(option);
            });
            
            arrayItemContainer.appendChild(arrayItemLabel);
            arrayItemContainer.appendChild(arrayItemSelect);
            
            // Event listeners for type changes
            typeSelect.addEventListener('change', function() {
                const newType = this.value;
                // Store current description before modifying the schema
                const currentDescription = currentSchema.properties[columnName].description || "";
                
                if (newType === 'array') {
                    // Show array item selector and set correct structure
                    arrayItemContainer.style.display = 'block';
                    const itemType = arrayItemSelect.value || 'string';
                    
                    // Check if column is required to determine structure
                    const isRequired = currentSchema.required && currentSchema.required.includes(columnName);
                    
                    if (isRequired) {
                        // Required array - simple structure
                        currentSchema.properties[columnName] = {
                            "type": "array",
                            "items": {"type": itemType},
                            "description": currentDescription
                        };
                    } else {
                        // Optional array - anyOf structure
                        currentSchema.properties[columnName] = {
                            "anyOf": [
                                {
                                    "type": "array",
                                    "items": {"type": itemType}
                                },
                                {"type": "null"}
                            ],
                            "description": currentDescription
                        };
                    }
                } else {
                    // Hide array item selector and set non-array type
                    arrayItemContainer.style.display = 'none';
                    
                    // Check if column is required to determine structure
                    const isRequired = currentSchema.required && currentSchema.required.includes(columnName);
                    
                    if (isRequired) {
                        // Required non-array - simple structure
                        currentSchema.properties[columnName] = {
                            "type": newType,
                            "description": currentDescription
                        };
                    } else {
                        // Optional non-array - anyOf structure
                        currentSchema.properties[columnName] = {
                            "anyOf": [
                                {"type": newType},
                                {"type": "null"}
                            ],
                            "description": currentDescription
                        };
                    }
                }
            });
            
            arrayItemSelect.addEventListener('change', function() {
                const itemType = this.value;
                
                // Check if this is a required array or optional array
                if (currentSchema.properties[columnName].type === 'array') {
                    // Required array - simple items structure
                    currentSchema.properties[columnName].items = {"type": itemType};
                } else if (currentSchema.properties[columnName].anyOf) {
                    // Optional array - update the array item in anyOf structure
                    const arrayItem = currentSchema.properties[columnName].anyOf.find(item => item.type === 'array');
                    if (arrayItem) {
                        arrayItem.items = {"type": itemType};
                    }
                }
            });
            
            typeContainer.appendChild(typeSelect);
            typeContainer.appendChild(arrayItemContainer);
            typeCell.appendChild(typeContainer);
            
            // Description (editable text input)
            const descCell = document.createElement('td');
            const descInput = document.createElement('input');
            descInput.type = 'text';
            descInput.className = 'form-control';
            descInput.value = columnSchema.description || '';
            descInput.dataset.column = columnName;
            
            descInput.addEventListener('change', function() {
                currentSchema.properties[columnName].description = this.value;
            });
            
            descCell.appendChild(descInput);
            
            // Required status (replacing sample data column)
            const requiredCell = document.createElement('td');
            const requiredCheckbox = document.createElement('input');
            requiredCheckbox.type = 'checkbox';
            requiredCheckbox.className = 'form-check-input';
            requiredCheckbox.checked = schema.required && schema.required.includes(columnName);
            
            requiredCheckbox.addEventListener('change', function() {
                const isRequired = this.checked;
                console.log(`Required toggle changed for ${columnName}: ${isRequired}`);
                
                // Store the current schema to send to backend (includes all local changes)
                const schemaToUpdate = JSON.parse(JSON.stringify(currentSchema));
                
                // Update the schema using the new format functions
                fetch('/update_schema_required', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        column_name: columnName,
                        is_required: isRequired,
                        current_schema: schemaToUpdate  // Send current schema with all local changes
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        // Update the current schema with the response (preserves all changes)
                        currentSchema = data.schema;
                        console.log(`Schema updated successfully: ${data.message}`);
                        console.log(`Required columns: ${currentSchema.required}`);
                    } else {
                        console.error('Error updating schema:', data.error);
                        // Revert the checkbox state on error
                        this.checked = !isRequired;
                        alert(`Error updating schema: ${data.error}`);
                    }
                })
                .catch(error => {
                    console.error('Error calling update_schema_required:', error);
                    // Revert the checkbox state on error
                    this.checked = !isRequired;
                    alert(`Error updating schema: ${error.message}`);
                });
            });
            
            const requiredLabel = document.createElement('label');
            requiredLabel.className = 'form-check-label ms-2';
            requiredLabel.textContent = 'Required';
            
            const requiredContainer = document.createElement('div');
            requiredContainer.className = 'form-check';
            requiredContainer.appendChild(requiredCheckbox);
            requiredContainer.appendChild(requiredLabel);
            
            requiredCell.appendChild(requiredContainer);
            
            // Add cells to row
            row.appendChild(nameCell);
            row.appendChild(typeCell);
            row.appendChild(descCell);
            row.appendChild(requiredCell);
            
            // Add row to table
            tableBody.appendChild(row);
        });
    }
    
    // Function to load saved schemas
    function loadSavedSchemas() {
        const schemasList = document.getElementById('savedSchemasList');
        schemasList.innerHTML = '<div class="list-group-item">Loading saved schemas...</div>';
        
        fetch('/list_schemas')
            .then(response => response.json())
            .then(data => {
                if (!data.success) {
                    schemasList.innerHTML = `<div class="list-group-item text-danger">${data.error || 'Error loading schemas'}</div>`;
                    return;
                }
                
                if (!data.schemas || data.schemas.length === 0) {
                    schemasList.innerHTML = '<div class="list-group-item">No saved schemas found</div>';
                    return;
                }
                
                // Display schemas
                schemasList.innerHTML = '';
                data.schemas.forEach(schema => {
                    const item = document.createElement('a');
                    item.className = 'list-group-item list-group-item-action d-flex justify-content-between align-items-center';
                    item.href = '#';
                    
                    // Create main content with timestamp
                    const mainContent = document.createElement('div');
                    const name = document.createElement('div');
                    name.textContent = schema.name;
                    name.className = 'fw-bold';
                    
                    const timestamp = document.createElement('small');
                    timestamp.className = 'text-muted';
                    timestamp.textContent = new Date(schema.timestamp).toLocaleString();
                    
                    mainContent.appendChild(name);
                    mainContent.appendChild(timestamp);
                    
                    // Create buttons container
                    const buttonsContainer = document.createElement('div');
                    
                    // Load button
                    const loadBtn = document.createElement('button');
                    loadBtn.className = 'btn btn-sm btn-primary me-2';
                    loadBtn.textContent = 'Load';
                    loadBtn.addEventListener('click', function(e) {
                        e.preventDefault();
                        e.stopPropagation();
                        loadSchema(schema.id);
                    });
                    
                    // Download button
                    const downloadBtn = document.createElement('button');
                    downloadBtn.className = 'btn btn-sm btn-success me-2';
                    downloadBtn.textContent = 'Download';
                    downloadBtn.addEventListener('click', function(e) {
                        e.preventDefault();
                        e.stopPropagation();
                        window.location.href = `/download_schema/${schema.id}`;
                    });
                    
                    // Delete button
                    const deleteBtn = document.createElement('button');
                    deleteBtn.className = 'btn btn-sm btn-danger';
                    deleteBtn.textContent = 'Delete';
                    deleteBtn.addEventListener('click', function(e) {
                        e.preventDefault();
                        e.stopPropagation();
                        deleteSchema(schema.id);
                    });
                    
                    buttonsContainer.appendChild(loadBtn);
                    buttonsContainer.appendChild(downloadBtn);
                    buttonsContainer.appendChild(deleteBtn);
                    
                    // Add both to the item
                    item.appendChild(mainContent);
                    item.appendChild(buttonsContainer);
                    
                    // Add to the list
                    schemasList.appendChild(item);
                });
            })
            .catch(error => {
                schemasList.innerHTML = `<div class="list-group-item text-danger">Error loading schemas: ${error.message}</div>`;
            });
    }
    
    // Function to load a schema
    function loadSchema(schemaId) {
        fetch(`/load_schema/${schemaId}`)
            .then(response => response.json())
            .then(data => {
                if (!data.success) {
                    alert(`Error: ${data.error || 'Could not load schema'}`);
                    return;
                }
                
                // Store and render the schema
                currentSchema = data.schema;
                renderSchemaTable(currentSchema);
            })
            .catch(error => {
                alert(`Error: ${error.message}`);
            });
    }
    
    // Function to delete a schema
    function deleteSchema(schemaId) {
        if (!confirm('Are you sure you want to delete this schema?')) {
            return;
        }
        
        // Ensure the schema ID is properly formatted
        const cleanSchemaId = schemaId.trim();
        console.log(`Attempting to delete schema: ${cleanSchemaId}`);
        
        fetch(`/delete_schema/${cleanSchemaId}`, {
            method: 'DELETE'
        })
            .then(response => {
                console.log(`Delete response status: ${response.status}`);
                return response.json();
            })
            .then(data => {
                console.log('Delete response data:', data);
                if (!data.success) {
                    alert(`Error: ${data.error || 'Could not delete schema'}`);
                    return;
                }
                
                alert('Schema deleted successfully');
                // Reload the schemas list
                loadSavedSchemas();
            })
            .catch(error => {
                console.error('Error deleting schema:', error);
                alert(`Error: ${error.message}`);
            });
    }
    
    // Event listeners for schema operations
    
    // Function to update currentSchema with all UI changes
    function updateCurrentSchemaFromUI() {
        if (!currentSchema) return;
        
        // Get all input fields and update currentSchema
        const tableBody = document.getElementById('schemaTableBody');
        const rows = tableBody.querySelectorAll('tr');
        
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 4) {
                const columnName = cells[0].textContent.trim();
                
                // Get type information
                const typeSelect = cells[1].querySelector('select[data-column]');
                const arrayItemSelect = cells[1].querySelector('.form-select-sm');
                
                // Get description
                const descInput = cells[2].querySelector('input[data-column]');
                
                // Get required status
                const requiredCheckbox = cells[3].querySelector('input[type="checkbox"]');
                
                if (columnName && currentSchema.properties[columnName]) {
                    // Update description
                    if (descInput) {
                        currentSchema.properties[columnName].description = descInput.value;
                    }
                    
                    // Update type and structure
                    if (typeSelect && requiredCheckbox) {
                        const newType = typeSelect.value;
                        const isRequired = requiredCheckbox.checked;
                        const currentDescription = currentSchema.properties[columnName].description || "";
                        
                        if (newType === 'array') {
                            const itemType = arrayItemSelect ? arrayItemSelect.value : 'string';
                            
                            if (isRequired) {
                                currentSchema.properties[columnName] = {
                                    "type": "array",
                                    "items": {"type": itemType},
                                    "description": currentDescription
                                };
                            } else {
                                currentSchema.properties[columnName] = {
                                    "anyOf": [
                                        {
                                            "type": "array",
                                            "items": {"type": itemType}
                                        },
                                        {"type": "null"}
                                    ],
                                    "description": currentDescription
                                };
                            }
                        } else {
                            if (isRequired) {
                                currentSchema.properties[columnName] = {
                                    "type": newType,
                                    "description": currentDescription
                                };
                            } else {
                                currentSchema.properties[columnName] = {
                                    "anyOf": [
                                        {"type": newType},
                                        {"type": "null"}
                                    ],
                                    "description": currentDescription
                                };
                            }
                        }
                        
                        // Update required array
                        if (!currentSchema.required) {
                            currentSchema.required = [];
                        }
                        
                        if (isRequired && !currentSchema.required.includes(columnName)) {
                            currentSchema.required.push(columnName);
                        } else if (!isRequired && currentSchema.required.includes(columnName)) {
                            currentSchema.required = currentSchema.required.filter(col => col !== columnName);
                        }
                    }
                }
            }
        });
    }

    // Save schema button
    document.getElementById('saveSchemaBtn').addEventListener('click', function() {
        const schemaName = document.getElementById('schemaName').value.trim();
        if (!schemaName) {
            alert('Please enter a schema name');
            return;
        }
        
        if (!currentSchema) {
            alert('No schema data available');
            return;
        }
        
        // Update currentSchema with all UI changes before saving
        updateCurrentSchemaFromUI();
        
        // Send request to save schema
        fetch('/save_schema', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                name: schemaName,
                schema: currentSchema
            })
        })
        .then(response => response.json())
        .then(data => {
            if (!data.success) {
                alert(`Error: ${data.error || 'Could not save schema'}`);
                return;
            }
            
            alert('Schema saved successfully');
            loadSavedSchemas();
        })
        .catch(error => {
            alert(`Error: ${error.message}`);
        });
    });
    
    // Download schema button
    document.getElementById('downloadSchemaBtn').addEventListener('click', function() {
        if (!currentSchema) {
            alert('No schema data available');
            return;
        }
        
        // Update currentSchema with all UI changes before downloading
        updateCurrentSchemaFromUI();
        
        // Create a download link for the schema JSON
        const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(currentSchema, null, 2));
        const downloadAnchorNode = document.createElement('a');
        downloadAnchorNode.setAttribute("href", dataStr);
        downloadAnchorNode.setAttribute("download", "schema.json");
        document.body.appendChild(downloadAnchorNode);
        downloadAnchorNode.click();
        downloadAnchorNode.remove();
    });
    
    // Upload schema input
    document.getElementById('uploadSchemaInput').addEventListener('change', function(e) {
        if (!this.files || !this.files[0]) return;
        
        const file = this.files[0];
        if (file.type !== 'application/json' && !file.name.endsWith('.json')) {
            alert('Please select a JSON file');
            return;
        }
        
        // Create form data
        const formData = new FormData();
        formData.append('schema_file', file);
        
        // Upload the schema
        fetch('/upload_schema', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (!data.success) {
                alert(`Error: ${data.error || 'Could not upload schema'}`);
                return;
            }
            
            // Store and render the schema
            currentSchema = data.schema;
            renderSchemaTable(currentSchema);
        })
        .catch(error => {
            alert(`Error: ${error.message}`);
        });
    });
    
    // Apply schema button
    document.getElementById('applySchemaBtn').addEventListener('click', function() {
        if (!currentSchema) {
            alert('No schema data available');
            return;
        }
        
        // Hide the modal
        schemaModal.hide();
        
        // Submit the form with the schema
        submitForm(currentSchema);
    });
});
