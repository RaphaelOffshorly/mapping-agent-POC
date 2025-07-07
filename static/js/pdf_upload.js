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

    // Schema Builder variables
    let currentSchema = null;
    const schemaModal = new bootstrap.Modal(document.getElementById('schemaBuilderModal'));
    
    // Array of Objects state management
    let schemaState = {
        isArrayOfObjects: false,
        arrayConfig: {
            name: "Invoice Data",
            description: "sample description"
        },
        originalSchema: null,  // Store original schema before conversion
        flatSchema: null       // Store flattened schema when in array mode
    };

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
        
        // First, extract target columns from the uploaded Excel file
        const formData = new FormData();
        formData.append('file', excelFile.files[0]);
        if (excelSheetSelect.value) {
            formData.append('sheet', excelSheetSelect.value);
        }
        
        // Step 1: Get target columns from the Excel file
        fetch('/get_target_columns', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                showError(data.error);
                return;
            }
            
            if (!data.target_columns || data.target_columns.length === 0) {
                showError('No target columns found in the Excel file');
                return;
            }
            
            // Step 2: Now create schema using the extracted target columns AND the Excel file
            const createSchemaFormData = new FormData();
            createSchemaFormData.append('target_columns', data.target_columns.join(', '));
            // Pass the Excel file as target_file so the schema builder can analyze the data
            createSchemaFormData.append('target_file', excelFile.files[0]);
            if (excelSheetSelect.value) {
                createSchemaFormData.append('target_sheet_name', excelSheetSelect.value);
            }
            
            return fetch('/create_schema', {
                method: 'POST',
                body: createSchemaFormData
            });
        })
        .then(response => {
            if (!response) return; // If there was an error in step 1
            return response.json();
        })
        .then(data => {
            if (!data) return; // If there was an error in step 1
            
            if (data.error) {
                showError(data.error);
            } else if (data.schema) {
                // Store the schema and open the schema builder modal
                currentSchema = data.schema;
                
                // Show loading state in modal
                document.getElementById('schemaTableBody').innerHTML = '<tr><td colspan="4" class="text-center">Loading schema data...</td></tr>';
                schemaModal.show();
                
                // Render schema table
                renderSchemaTable(currentSchema);
                
                // Load saved schemas list
                loadSavedSchemas();
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

    // Handle save schema button (old functionality - keeping for backward compatibility)
    const saveSchemaOldBtn = document.getElementById('saveSchemaOldBtn');
    saveSchemaOldBtn.addEventListener('click', function() {
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
        
        // Add schema if we have one
        if (currentSchema) {
            console.log('PDF SUBMISSION DEBUG: Current schema being sent:');
            console.log(JSON.stringify(currentSchema, null, 2));
            console.log('PDF SUBMISSION DEBUG: Array mode active:', schemaState.isArrayOfObjects);
            console.log('PDF SUBMISSION DEBUG: Array config:', schemaState.arrayConfig);
            formData.append('schema', JSON.stringify(currentSchema));
        } else {
            console.error('PDF SUBMISSION DEBUG: No currentSchema available!');
        }
        
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

    // Schema Builder Functions (copied from main.js and adapted)

    // Function to render the schema table
    function renderSchemaTable(schema) {
        const tableBody = document.getElementById('schemaTableBody');
        tableBody.innerHTML = '';
        
        console.log('RENDER SCHEMA TABLE DEBUG: Starting render with schema:');
        console.log(JSON.stringify(schema, null, 2));
        console.log('RENDER SCHEMA TABLE DEBUG: Array mode active:', schemaState.isArrayOfObjects);
        
        // Determine properties to render based on schema type
        let propertiesToRender = {};
        let requiredFields = [];
        
        if (schemaState.isArrayOfObjects && schema.properties) {
            // In array mode, render the properties from the array items
            const arrayPropertyName = Object.keys(schema.properties)[0];
            const arrayProperty = schema.properties[arrayPropertyName];
            
            console.log('RENDER SCHEMA TABLE DEBUG: Array property name:', arrayPropertyName);
            console.log('RENDER SCHEMA TABLE DEBUG: Array property:', JSON.stringify(arrayProperty, null, 2));
            
            if (arrayProperty && arrayProperty.items && arrayProperty.items.properties) {
                propertiesToRender = arrayProperty.items.properties;
                requiredFields = arrayProperty.items.required || [];
                console.log('RENDER SCHEMA TABLE DEBUG: Rendering array items properties:', Object.keys(propertiesToRender));
            } else {
                console.error('RENDER SCHEMA TABLE DEBUG: Array property missing items or properties!');
                propertiesToRender = schema.properties;
                requiredFields = schema.required || [];
            }
        } else {
            // Regular mode, render schema properties directly
            propertiesToRender = schema.properties;
            requiredFields = schema.required || [];
            console.log('RENDER SCHEMA TABLE DEBUG: Rendering regular properties:', Object.keys(propertiesToRender));
        }
        
        // For each property to render
        Object.entries(propertiesToRender).forEach(([columnName, columnSchema]) => {
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
                
                if (schemaState.isArrayOfObjects) {
                    // For array mode, update the type in the array items properties
                    const arrayPropertyName = Object.keys(currentSchema.properties)[0];
                    const arrayProperty = currentSchema.properties[arrayPropertyName];
                    
                    if (arrayProperty && arrayProperty.items && arrayProperty.items.properties && arrayProperty.items.properties[columnName]) {
                        const currentDescription = arrayProperty.items.properties[columnName].description || "";
                        const isRequired = arrayProperty.items.required && arrayProperty.items.required.includes(columnName);
                        
                        if (newType === 'array') {
                            // Show array item selector - but in array mode, this becomes a nested array
                            arrayItemContainer.style.display = 'block';
                            const itemType = arrayItemSelect.value || 'string';
                            
                            if (isRequired) {
                                arrayProperty.items.properties[columnName] = {
                                    "type": "array",
                                    "items": {"type": itemType},
                                    "description": currentDescription
                                };
                            } else {
                                arrayProperty.items.properties[columnName] = {
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
                            
                            if (isRequired) {
                                arrayProperty.items.properties[columnName] = {
                                    "type": newType,
                                    "description": currentDescription
                                };
                            } else {
                                arrayProperty.items.properties[columnName] = {
                                    "anyOf": [
                                        {"type": newType},
                                        {"type": "null"}
                                    ],
                                    "description": currentDescription
                                };
                            }
                        }
                        
                        console.log(`Updated array item property ${columnName} type to: ${newType}`);
                    }
                } else {
                    // For regular mode, use the existing logic
                    const currentDescription = currentSchema.properties[columnName].description || "";
                    
                    if (newType === 'array') {
                        arrayItemContainer.style.display = 'block';
                        const itemType = arrayItemSelect.value || 'string';
                        const isRequired = currentSchema.required && currentSchema.required.includes(columnName);
                        
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
                        arrayItemContainer.style.display = 'none';
                        const isRequired = currentSchema.required && currentSchema.required.includes(columnName);
                        
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
                }
            });
            
            arrayItemSelect.addEventListener('change', function() {
                const itemType = this.value;
                
                if (schemaState.isArrayOfObjects) {
                    // For array mode, update the array item type in the array items properties
                    const arrayPropertyName = Object.keys(currentSchema.properties)[0];
                    const arrayProperty = currentSchema.properties[arrayPropertyName];
                    
                    if (arrayProperty && arrayProperty.items && arrayProperty.items.properties && arrayProperty.items.properties[columnName]) {
                        const propSchema = arrayProperty.items.properties[columnName];
                        
                        if (propSchema.type === 'array') {
                            propSchema.items = {"type": itemType};
                        } else if (propSchema.anyOf) {
                            const arrayItem = propSchema.anyOf.find(item => item.type === 'array');
                            if (arrayItem) {
                                arrayItem.items = {"type": itemType};
                            }
                        }
                        
                        console.log(`Updated array item property ${columnName} array item type to: ${itemType}`);
                    }
                } else {
                    // For regular mode, use the existing logic
                    if (currentSchema.properties[columnName].type === 'array') {
                        currentSchema.properties[columnName].items = {"type": itemType};
                    } else if (currentSchema.properties[columnName].anyOf) {
                        const arrayItem = currentSchema.properties[columnName].anyOf.find(item => item.type === 'array');
                        if (arrayItem) {
                            arrayItem.items = {"type": itemType};
                        }
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
                if (schemaState.isArrayOfObjects) {
                    // For array mode, update the description in the array items properties
                    const arrayPropertyName = Object.keys(currentSchema.properties)[0];
                    const arrayProperty = currentSchema.properties[arrayPropertyName];
                    
                    if (arrayProperty && arrayProperty.items && arrayProperty.items.properties && arrayProperty.items.properties[columnName]) {
                        arrayProperty.items.properties[columnName].description = this.value;
                        console.log(`Updated array item property ${columnName} description to: ${this.value}`);
                    }
                } else {
                    // For regular mode, update the description in the main properties
                    currentSchema.properties[columnName].description = this.value;
                }
            });
            
            descCell.appendChild(descInput);
            
            // Required status (replacing sample data column)
            const requiredCell = document.createElement('td');
            const requiredCheckbox = document.createElement('input');
            requiredCheckbox.type = 'checkbox';
            requiredCheckbox.className = 'form-check-input';
            requiredCheckbox.checked = requiredFields.includes(columnName);
            
            requiredCheckbox.addEventListener('change', function() {
                const isRequired = this.checked;
                console.log(`Required toggle changed for ${columnName}: ${isRequired}`);
                
                if (schemaState.isArrayOfObjects) {
                    // For array mode, update the required fields in the array items
                    const arrayPropertyName = Object.keys(currentSchema.properties)[0];
                    const arrayProperty = currentSchema.properties[arrayPropertyName];
                    
                    if (arrayProperty && arrayProperty.items) {
                        if (!arrayProperty.items.required) {
                            arrayProperty.items.required = [];
                        }
                        
                        if (isRequired && !arrayProperty.items.required.includes(columnName)) {
                            arrayProperty.items.required.push(columnName);
                        } else if (!isRequired && arrayProperty.items.required.includes(columnName)) {
                            arrayProperty.items.required = arrayProperty.items.required.filter(col => col !== columnName);
                        }
                        
                        console.log('Updated array items required fields:', arrayProperty.items.required);
                    }
                } else {
                    // For regular mode, use the backend update
                    const schemaToUpdate = JSON.parse(JSON.stringify(currentSchema));
                    
                    fetch('/update_schema_required', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            column_name: columnName,
                            is_required: isRequired,
                            current_schema: schemaToUpdate
                        })
                    })
                    .then(response => response.json())
                    .then(data => {
                        if (data.success) {
                            currentSchema = data.schema;
                            console.log(`Schema updated successfully: ${data.message}`);
                            console.log(`Required columns: ${currentSchema.required}`);
                        } else {
                            console.error('Error updating schema:', data.error);
                            this.checked = !isRequired;
                            alert(`Error updating schema: ${data.error}`);
                        }
                    })
                    .catch(error => {
                        console.error('Error calling update_schema_required:', error);
                        this.checked = !isRequired;
                        alert(`Error updating schema: ${error.message}`);
                    });
                }
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
        
        // Load schemas for PDF mode (includes both regular and array schemas)
        fetch('/list_schemas?mode=pdf')
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
                    
                    // Create main content with timestamp and type indicator
                    const mainContent = document.createElement('div');
                    const nameContainer = document.createElement('div');
                    nameContainer.className = 'd-flex align-items-center';
                    
                    const name = document.createElement('span');
                    name.textContent = schema.name;
                    name.className = 'fw-bold';
                    nameContainer.appendChild(name);
                    
                    // Add array indicator badge if this is an array schema
                    if (schema.is_array_of_objects) {
                        const arrayBadge = document.createElement('span');
                        arrayBadge.className = 'badge bg-info ms-2';
                        arrayBadge.textContent = 'Array';
                        arrayBadge.title = 'Array of Objects Schema';
                        nameContainer.appendChild(arrayBadge);
                    }
                    
                    const timestamp = document.createElement('small');
                    timestamp.className = 'text-muted';
                    timestamp.textContent = new Date(schema.timestamp).toLocaleString();
                    
                    mainContent.appendChild(nameContainer);
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
                        loadSchema(schema.id, schema.is_array_of_objects, schema.array_config);
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
    function loadSchema(schemaId, isArraySchema = false, arrayConfig = null) {
        fetch(`/load_schema/${schemaId}`)
            .then(response => response.json())
            .then(data => {
                if (!data.success) {
                    alert(`Error: ${data.error || 'Could not load schema'}`);
                    return;
                }
                
                // Store and render the schema
                currentSchema = data.schema;
                
                // If this is an array schema, set up the array state
                if (isArraySchema && arrayConfig) {
                    // First render the schema as-is (array format)
                    renderSchemaTable(currentSchema);
                    
                    // Extract the inner schema for state management
                    const arrayPropertyName = Object.keys(currentSchema.properties)[0];
                    const arrayProperty = currentSchema.properties[arrayPropertyName];
                    const innerSchema = {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": arrayProperty.items.properties,
                        "required": arrayProperty.items.required || []
                    };
                    
                    // Set up array state
                    schemaState.isArrayOfObjects = true;
                    schemaState.originalSchema = innerSchema;
                    schemaState.arrayConfig = {
                        name: arrayConfig.name || arrayPropertyName,
                        description: arrayConfig.description || arrayProperty.description || ""
                    };
                    
                    // Update UI to reflect array mode
                    arrayOfObjectsToggle.checked = true;
                    arrayNameInput.value = schemaState.arrayConfig.name;
                    arrayDescriptionInput.value = schemaState.arrayConfig.description;
                    arrayConfigFields.classList.remove('d-none');
                    arrayModeWarning.classList.remove('d-none');
                    
                    // Disable array type options in the UI
                    updateTypeSelectorsForArrayMode(true);
                    
                    console.log('Loaded array schema:', currentSchema);
                    console.log('Array state set up:', schemaState);
                } else {
                    // Regular schema - ensure array mode is off
                    schemaState.isArrayOfObjects = false;
                    schemaState.originalSchema = null;
                    arrayOfObjectsToggle.checked = false;
                    arrayConfigFields.classList.add('d-none');
                    arrayModeWarning.classList.add('d-none');
                    
                    // Re-enable array type options in the UI
                    updateTypeSelectorsForArrayMode(false);
                    
                    // Render the schema
                    renderSchemaTable(currentSchema);
                    
                    console.log('Loaded regular schema:', currentSchema);
                }
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
    
    // Event listeners for schema operations in modal
    
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

    // Save schema button in modal
    document.getElementById('saveSchemaBtn').addEventListener('click', function(e) {
        console.log('PDF Mode: Save schema button clicked');
        e.preventDefault();
        e.stopPropagation();
        
        const schemaName = document.getElementById('schemaName').value.trim();
        console.log('PDF Mode: Schema name:', schemaName);
        
        if (!schemaName) {
            alert('Please enter a schema name');
            return;
        }
        
        if (!currentSchema) {
            alert('No schema data available - current schema is: ' + JSON.stringify(currentSchema));
            return;
        }
        
        // Update currentSchema with all UI changes before saving
        updateCurrentSchemaFromUI();
        
        console.log('PDF Mode: Updated schema with UI changes:', currentSchema);
        console.log('PDF Mode: Sending save request...');
        
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
        .then(response => {
            console.log('PDF Mode: Save response status:', response.status);
            return response.json();
        })
        .then(data => {
            console.log('PDF Mode: Save response data:', data);
            if (!data.success) {
                alert(`Error: ${data.error || 'Could not save schema'}`);
                return;
            }
            
            alert('Schema saved successfully');
            loadSavedSchemas();
            
            // Clear the schema name
            document.getElementById('schemaName').value = '';
        })
        .catch(error => {
            console.error('PDF Mode: Save error:', error);
            alert(`Error: ${error.message}`);
        });
    });
    
    // Download schema button in modal
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
    
    // Upload schema input in modal
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
    
    // Array of Objects Toggle Event Listeners
    const arrayOfObjectsToggle = document.getElementById('arrayOfObjectsToggle');
    const arrayConfigFields = document.getElementById('arrayConfigFields');
    const arrayModeWarning = document.getElementById('arrayModeWarning');
    const arrayNameInput = document.getElementById('arrayName');
    const arrayDescriptionInput = document.getElementById('arrayDescription');

    // Array toggle event listener
    arrayOfObjectsToggle.addEventListener('change', function() {
        const isEnabled = this.checked;
        console.log('Array of Objects toggle changed to:', isEnabled);
        
        if (isEnabled) {
            enableArrayOfObjectsMode();
        } else {
            disableArrayOfObjectsMode();
        }
    });

    // Array name and description change listeners
    arrayNameInput.addEventListener('change', function() {
        console.log('ARRAY NAME INPUT CHANGE - Before update:');
        console.log('  - New name:', this.value);
        console.log('  - Current schema:', JSON.stringify(currentSchema, null, 2));
        console.log('  - Array mode active:', schemaState.isArrayOfObjects);
        
        schemaState.arrayConfig.name = this.value;
        if (schemaState.isArrayOfObjects) {
            // Update the current array schema
            updateArraySchemaConfig();
        }
        
        console.log('ARRAY NAME INPUT CHANGE - After update:');
        console.log('  - Final schema:', JSON.stringify(currentSchema, null, 2));
    });

    arrayDescriptionInput.addEventListener('change', function() {
        console.log('ARRAY DESCRIPTION INPUT CHANGE - Before update:');
        console.log('  - New description:', this.value);
        console.log('  - Current schema:', JSON.stringify(currentSchema, null, 2));
        
        schemaState.arrayConfig.description = this.value;
        if (schemaState.isArrayOfObjects) {
            // Update the current array schema
            updateArraySchemaConfig();
        }
        
        console.log('ARRAY DESCRIPTION INPUT CHANGE - After update:');
        console.log('  - Final schema:', JSON.stringify(currentSchema, null, 2));
    });

    // Array of Objects functions
    function enableArrayOfObjectsMode() {
        if (!currentSchema) {
            alert('No schema available to convert');
            arrayOfObjectsToggle.checked = false;
            return;
        }

        // Store original schema before conversion
        schemaState.originalSchema = JSON.parse(JSON.stringify(currentSchema));
        schemaState.isArrayOfObjects = true;

        // Show array configuration fields and warning
        arrayConfigFields.classList.remove('d-none');
        arrayModeWarning.classList.remove('d-none');

        // Update array config from inputs
        schemaState.arrayConfig.name = arrayNameInput.value;
        schemaState.arrayConfig.description = arrayDescriptionInput.value;

        // Convert schema to array of objects format
        try {
            currentSchema = convertToArrayOfObjectsSchema(currentSchema, schemaState.arrayConfig.name, schemaState.arrayConfig.description);
            
            // Re-render the table to show the new structure
            renderSchemaTable(currentSchema);
            
            // Disable array type options in the UI
            updateTypeSelectorsForArrayMode(true);
            
            console.log('Converted to array of objects schema:', currentSchema);
        } catch (error) {
            console.error('Error converting to array schema:', error);
            alert('Error converting to array schema: ' + error.message);
            disableArrayOfObjectsMode();
        }
    }

    function disableArrayOfObjectsMode() {
        if (!schemaState.originalSchema) {
            console.log('No original schema to restore');
            return;
        }

        schemaState.isArrayOfObjects = false;

        // Hide array configuration fields and warning
        arrayConfigFields.classList.add('d-none');
        arrayModeWarning.classList.add('d-none');

        // Restore original schema
        currentSchema = JSON.parse(JSON.stringify(schemaState.originalSchema));
        schemaState.originalSchema = null;

        // Re-render the table to show the restored structure
        renderSchemaTable(currentSchema);
        
        // Re-enable array type options in the UI
        updateTypeSelectorsForArrayMode(false);
        
        console.log('Restored original schema:', currentSchema);
    }

    function convertToArrayOfObjectsSchema(schema, arrayName, arrayDescription) {
        console.log('convertToArrayOfObjectsSchema called with:');
        console.log('  - Original schema:', JSON.stringify(schema, null, 2));
        console.log('  - Array name:', arrayName);
        console.log('  - Array description:', arrayDescription);
        
        if (!schema || !schema.properties) {
            console.error('Invalid schema passed to convertToArrayOfObjectsSchema:', schema);
            throw new Error('Invalid schema: missing properties');
        }
        
        // Flatten array properties to their base types
        const flattened_properties = flattenArrayProperties(schema.properties);
        console.log('  - Flattened properties:', JSON.stringify(flattened_properties, null, 2));
        
        // Create array of objects schema matching the sample structure
        const arraySchema = {
            "type": "object",
            "additionalProperties": false,
            "properties": {
                [arrayName]: {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": flattened_properties,
                        "required": schema.required || []
                    }
                }
            },
            "required": [arrayName]
        };
        
        console.log('  - Final array schema:', JSON.stringify(arraySchema, null, 2));
        return arraySchema;
    }

    function flattenArrayProperties(properties) {
        console.log('flattenArrayProperties called with:', JSON.stringify(properties, null, 2));
        const flattened = {};
        
        for (const [propName, propSchema] of Object.entries(properties)) {
            console.log(`Processing property '${propName}':`, JSON.stringify(propSchema, null, 2));
            let newProp = { ...propSchema };
            
            // Handle direct array type
            if (propSchema.type === 'array') {
                const items = propSchema.items || {};
                const baseType = items.type || 'string';
                
                newProp = {
                    "type": baseType,
                    "description": propSchema.description || ""
                };
                console.log(`Flattened array property '${propName}' from array to ${baseType}`);
            }
            // Handle anyOf structure with arrays
            else if (propSchema.anyOf) {
                const arrayItem = propSchema.anyOf.find(item => item.type === 'array');
                const nullItem = propSchema.anyOf.find(item => item.type === 'null');
                
                if (arrayItem) {
                    const items = arrayItem.items || {};
                    const baseType = items.type || 'string';
                    
                    if (nullItem) {
                        // Optional field - use anyOf with base type and null
                        newProp = {
                            "anyOf": [
                                {"type": baseType},
                                {"type": "null"}
                            ],
                            "description": propSchema.description || ""
                        };
                    } else {
                        // Required field - use base type directly
                        newProp = {
                            "type": baseType,
                            "description": propSchema.description || ""
                        };
                    }
                    console.log(`Flattened anyOf array property '${propName}' to ${baseType}`);
                } else {
                    console.log(`Property '${propName}' has anyOf but no array item found`);
                }
            } else {
                console.log(`Property '${propName}' kept as-is (type: ${propSchema.type})`);
            }
            
            flattened[propName] = newProp;
            console.log(`Final flattened property '${propName}':`, JSON.stringify(newProp, null, 2));
        }
        
        console.log('Final flattened result:', JSON.stringify(flattened, null, 2));
        return flattened;
    }

    function updateArraySchemaConfig() {
        if (!schemaState.isArrayOfObjects || !currentSchema) return;
        
        console.log('updateArraySchemaConfig called - current schema before update:');
        console.log(JSON.stringify(currentSchema, null, 2));
        
        // Update the array name and description in the current schema
        const arrayName = schemaState.arrayConfig.name;
        const arrayDescription = schemaState.arrayConfig.description;
        
        // Get the current array property
        const currentArrayName = Object.keys(currentSchema.properties)[0];
        const arrayProperty = currentSchema.properties[currentArrayName];
        
        console.log('updateArraySchemaConfig - arrayName:', arrayName, 'currentArrayName:', currentArrayName);
        console.log('updateArraySchemaConfig - arrayProperty:', JSON.stringify(arrayProperty, null, 2));
        
        // IMPORTANT: Don't set description directly on arrayProperty as it will corrupt the structure
        // The array structure should remain: { type: "array", items: { ... } }
        // Description should be optional and only added if it's not empty
        
        // If the name changed, we need to rename the property
        if (currentArrayName !== arrayName) {
            console.log('updateArraySchemaConfig - arrayProperty.items before copy:', JSON.stringify(arrayProperty.items, null, 2));
            
            // Ensure we have the items structure
            if (!arrayProperty.items) {
                console.error('updateArraySchemaConfig - ERROR: arrayProperty.items is missing!');
                console.error('updateArraySchemaConfig - Current arrayProperty:', JSON.stringify(arrayProperty, null, 2));
                return; // Don't proceed if items is missing
            }
            
            // Create new property with the correct structure, preserving items completely
            const newArrayProperty = {
                "type": "array",
                "items": JSON.parse(JSON.stringify(arrayProperty.items)) // Deep clone to preserve structure
            };
            
            // Only add description if it's not empty
            if (arrayDescription && arrayDescription.trim() !== "") {
                newArrayProperty.description = arrayDescription;
            }
            
            console.log('updateArraySchemaConfig - newArrayProperty created:', JSON.stringify(newArrayProperty, null, 2));
            
            // Update the schema
            delete currentSchema.properties[currentArrayName];
            currentSchema.properties[arrayName] = newArrayProperty;
            currentSchema.required = [arrayName];
            
            console.log('updateArraySchemaConfig - renamed array property');
        } else {
            // Just update the description if name hasn't changed
            if (arrayDescription && arrayDescription.trim() !== "") {
                arrayProperty.description = arrayDescription;
            } else {
                // Remove description if it's empty
                delete arrayProperty.description;
            }
            console.log('updateArraySchemaConfig - updated description only');
        }
        
        console.log('updateArraySchemaConfig - final schema:');
        console.log(JSON.stringify(currentSchema, null, 2));
    }

    function updateTypeSelectorsForArrayMode(isArrayMode) {
        const typeSelectors = document.querySelectorAll('#schemaTable select[data-column]');
        
        typeSelectors.forEach(selector => {
            const arrayOption = selector.querySelector('option[value="array"]');
            if (arrayOption) {
                arrayOption.disabled = isArrayMode;
                arrayOption.style.display = isArrayMode ? 'none' : 'block';
            }
            
            // If currently selected as array and we're enabling array mode, change to string
            if (isArrayMode && selector.value === 'array') {
                selector.value = 'string';
                // Trigger change event to update the schema
                selector.dispatchEvent(new Event('change'));
            }
        });
    }

    // Apply schema button in modal
    document.getElementById('applySchemaBtn').addEventListener('click', function() {
        if (!currentSchema) {
            alert('No schema data available');
            return;
        }
        
        console.log('APPLY SCHEMA DEBUG: Schema before updateCurrentSchemaFromUI:');
        console.log(JSON.stringify(currentSchema, null, 2));
        console.log('APPLY SCHEMA DEBUG: Array mode active:', schemaState.isArrayOfObjects);
        
        // Update currentSchema with all UI changes before applying
        if (schemaState.isArrayOfObjects) {
            console.log('APPLY SCHEMA DEBUG: In array mode - skipping updateCurrentSchemaFromUI to preserve array structure');
            // Don't call updateCurrentSchemaFromUI for array schemas as it will break the structure
        } else {
            console.log('APPLY SCHEMA DEBUG: In regular mode - calling updateCurrentSchemaFromUI');
            updateCurrentSchemaFromUI();
        }
        
        console.log('APPLY SCHEMA DEBUG: Final schema after processing:');
        console.log(JSON.stringify(currentSchema, null, 2));
        
        // Hide the modal
        schemaModal.hide();
        
        // Hide step 1 and show step 3 (skip step 2 entirely)
        step1Container.classList.add('d-none');
        step3Container.classList.remove('d-none');
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
