// Store selected cells for each target column
const selectedCellsByTarget = {};

document.addEventListener('DOMContentLoaded', function() {
    // Check if Bootstrap is available
    console.log("DOM Content Loaded");
    console.log("Bootstrap available:", typeof bootstrap !== 'undefined');
    if (typeof bootstrap !== 'undefined') {
        console.log("Bootstrap version:", bootstrap.Tooltip.VERSION);
    } else {
        console.error("Bootstrap JavaScript is not loaded. Modal functionality won't work.");
    }
    
    // Form for adding new header
    const addHeaderForm = document.getElementById('addHeaderForm');
    const addHeaderSpinner = document.getElementById('addHeaderSpinner');
    const addHeaderError = document.getElementById('addHeaderError');
    const addHeaderSuccess = document.getElementById('addHeaderSuccess');
    
    // CSV preview elements
    const csvTab = document.getElementById('csv-tab');
    const csvPreviewContainer = document.getElementById('csvPreviewContainer');
    const csvLoadingIndicator = document.getElementById('csvLoadingIndicator');
    
    // Re-analyze all button
    const reAnalyzeAllBtn = document.getElementById('reAnalyzeAll');
    const reAnalyzeSpinner = document.getElementById('reAnalyzeSpinner');
    
    // Individual re-match buttons
    const reMatchButtons = document.querySelectorAll('.re-match-btn');
    
    // Suggest data buttons
    const suggestDataButtons = document.querySelectorAll('.suggest-data-btn');
    
    // Suggest header buttons
    const suggestHeaderButtons = document.querySelectorAll('.suggest-header-btn');
    
    // Revert data buttons
    const revertDataButtons = document.querySelectorAll('.revert-data-btn');
    
    // Store original sample data for revert functionality
    let originalSampleData = {};
    
    // Store AI suggested data for export
    const aiSuggestedData = {};
    
    // Data selection buttons
    const selectDataButtons = document.querySelectorAll('.select-data-btn');
    
    // Excel preview elements
    const excelSheetTabs = document.getElementById('excelSheetTabs');
    const excelSheetContent = document.getElementById('excelSheetContent');
    const excelLoadingIndicator = document.getElementById('excelLoadingIndicator');
    const loadMoreContainer = document.getElementById('loadMoreContainer');
    const loadMoreButton = document.getElementById('loadMoreButton');
    const loadMoreSpinner = document.getElementById('loadMoreSpinner');
    
    // Excel selection variables
    let selectionStart = null;
    let selectionEnd = null;
    let currentSheet = null;
    let currentTarget = null;
    let currentHeader = null;
    let selectedCells = [];
    
    // Excel data state
    let currentSheetName = '';
    let currentStartRow = 0;
    let rowsPerPage = 50;
    let hasMoreRows = false;
    let sheetNames = [];
    let totalRows = 0;
    let totalCols = 0;
    
    // Load initial Excel data when page loads
    loadExcelPreview();
    
    // Initialize sample data from the table
    initializeSampleData();
    
// Setup AI Chatbot
console.log("Setting up AI Chatbot...");
setupAIChatbot();

// Make generateCsvPreview globally accessible
window.generateCsvPreview = generateCsvPreview;
    
    // Function to initialize sample data from the table
    function initializeSampleData() {
        console.log("Initializing sample data...");
        const rows = document.querySelectorAll('.table-striped tbody tr');
        
        rows.forEach(row => {
            const targetCell = row.querySelector('td:first-child');
            if (!targetCell) return;
            
            const targetColumn = targetCell.textContent.trim();
            const sampleDataCell = row.querySelector('td:nth-child(5)');
            
            if (sampleDataCell) {
                const sampleItems = Array.from(sampleDataCell.querySelectorAll('.sample-item'))
                    .map(item => item.textContent.trim());
                
                if (sampleItems.length > 0) {
                    console.log(`Found ${sampleItems.length} sample items for ${targetColumn}`);
                    originalSampleData[targetColumn] = sampleItems;
                }
            }
        });
        
        console.log("Original sample data:", originalSampleData);
    }
    
    // Add event listener for CSV tab to generate preview when clicked
    if (csvTab) {
        csvTab.addEventListener('click', function() {
            console.log("CSV tab clicked, generating preview...");
            // Hide the load more button when CSV tab is active
            loadMoreContainer.classList.add('d-none');
            generateCsvPreview();
        });
    }
    
    // Add event listener for Excel tab to show load more button if needed
    document.getElementById('excel-tab').addEventListener('click', function() {
        // Show load more button if there are more rows
        if (hasMoreRows) {
            loadMoreContainer.classList.remove('d-none');
        }
    });
    
    // Add event listener for radio button changes to update CSV preview
    document.addEventListener('change', function(e) {
        if (e.target.classList.contains('data-radio')) {
            console.log("Radio button selection changed:", e.target.getAttribute('data-target'), e.target.getAttribute('data-source'));
            
            // Always regenerate the preview if the CSV tab is active
            if (document.getElementById('csv-content').classList.contains('active')) {
                generateCsvPreview();
            }
            
            // Store the selection in aiSuggestedData if it's an AI selection
            const targetColumn = e.target.getAttribute('data-target');
            const dataSource = e.target.getAttribute('data-source');
            
            if (dataSource === 'ai' && targetColumn) {
                // Make sure the AI data is stored for this target column
                if (!aiSuggestedData[targetColumn]) {
                    // Try to find the AI data from the DOM
                    const aiDataCell = document.getElementById(`aiCell-${targetColumn.replace(/ /g, '_')}`);
                    if (aiDataCell) {
                        const sampleItems = Array.from(aiDataCell.querySelectorAll('.sample-item'))
                            .map(item => item.textContent.trim());
                        if (sampleItems.length > 0) {
                            aiSuggestedData[targetColumn] = sampleItems;
                            console.log(`Stored AI data for ${targetColumn}:`, sampleItems);
                        }
                    }
                }
            }
        }
    });
    
    // Handle adding a new header
    if (addHeaderForm) {
        addHeaderForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            // Hide previous messages
            addHeaderError.classList.add('d-none');
            addHeaderSuccess.classList.add('d-none');
            
            // Show spinner
            addHeaderSpinner.classList.remove('d-none');
            
            const newHeader = document.getElementById('newHeader').value.trim();
            if (!newHeader) {
                addHeaderError.textContent = 'Please enter a header name';
                addHeaderError.classList.remove('d-none');
                addHeaderSpinner.classList.add('d-none');
                return;
            }
            
            // Send request to add header
            fetch('/add_header', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ header: newHeader })
            })
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    addHeaderError.textContent = data.error;
                    addHeaderError.classList.remove('d-none');
                } else {
                    // Success - show message and update UI
                    addHeaderSuccess.textContent = 'Header added successfully!';
                    addHeaderSuccess.classList.remove('d-none');
                    
                    // Add new header to the displayed list
                    const headersContainer = document.querySelector('.detected-headers');
                    const newBadge = document.createElement('span');
                    newBadge.className = 'badge bg-success mb-1 me-1';
                    newBadge.textContent = newHeader;
                    headersContainer.appendChild(newBadge);
                    
                    // Clear the input
                    document.getElementById('newHeader').value = '';
                    
                    // Auto-trigger re-analyze after short delay
                    setTimeout(() => {
                        triggerReAnalyze();
                    }, 1000);
                }
            })
            .catch(error => {
                addHeaderError.textContent = 'An error occurred: ' + error.message;
                addHeaderError.classList.remove('d-none');
            })
            .finally(() => {
                addHeaderSpinner.classList.add('d-none');
            });
        });
    }
    
    // Handle re-matching individual target
    reMatchButtons.forEach(button => {
        button.addEventListener('click', function() {
            const targetColumn = this.getAttribute('data-target');
            
            // Disable button and show spinner
            this.disabled = true;
            const originalText = this.textContent;
            this.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Matching...';
            
            // Send request to re-match this target
            fetch('/re_match', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ target_column: targetColumn })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Refresh the page to show updated results
                    window.location.reload();
                } else {
                    // Reset button and show error
                    this.textContent = originalText;
                    this.disabled = false;
                    alert(data.error || 'An error occurred while re-matching.');
                }
            })
            .catch(error => {
                this.textContent = originalText;
                this.disabled = false;
                alert('An error occurred: ' + error.message);
            });
        });
    });
    
    // Use event delegation for suggest data buttons
    document.addEventListener('click', function(e) {
        // Check if the clicked element is a suggest data button or a child of it
        const button = e.target.closest('.suggest-data-btn');
        if (!button) return; // Not a suggest data button
        
        // Skip disabled buttons
        if (button.classList.contains('disabled') || button.hasAttribute('disabled')) {
            return;
        }
        
        const targetColumn = button.getAttribute('data-target');
        console.log("Suggesting data for column:", targetColumn);
        
        // Disable button and show spinner
        button.disabled = true;
        const originalHtml = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Suggesting...';
        
        // Find the matching row by target column name
        // Use a more specific selector to ensure we get the correct row
        const allRows = document.querySelectorAll('.table-striped tbody tr');
        console.log("Total rows in table:", allRows.length);
        
        // Log all target columns for debugging
        console.log("All target columns in table:");
        allRows.forEach((row, index) => {
            const targetCell = row.querySelector('td:first-child');
            if (targetCell) {
                console.log(`Row ${index}: ${targetCell.textContent.trim()}`);
            }
        });
        
        // Find the exact row for this target column
        const matchingRow = Array.from(allRows).find(row => {
            const targetCell = row.querySelector('td:first-child');
            return targetCell && targetCell.textContent.trim() === targetColumn;
        });
        
        console.log("Found matching row for", targetColumn, ":", !!matchingRow);
        
        if (!matchingRow) {
            // Reset button and show error
            button.innerHTML = originalHtml;
            button.disabled = false;
            alert('Error: Could not find the matching row for this column.');
            return;
        }
        
        // Safely check if this is a "No match found" row
        let isNoMatch = false;
        try {
            const matchedHeaderCell = matchingRow.querySelector('td:nth-child(2)');
            if (matchedHeaderCell) {
                isNoMatch = matchedHeaderCell.textContent.trim() === "No match found";
            }
        } catch (e) {
            console.error("Error checking if row is 'No match found':", e);
        }
        
        console.log("Is 'No match found' row in suggest_sample_data:", isNoMatch);
        
        const sampleDataCell = matchingRow.querySelector('td:nth-child(4)');
        if (sampleDataCell) {
            // Store original sample data for revert functionality
            const sampleItems = Array.from(sampleDataCell.querySelectorAll('.sample-item')).map(item => item.textContent.trim());
            if (sampleItems.length > 0) {
                originalSampleData[targetColumn] = sampleItems;
            }
        }
        
        // Send request to suggest data for this target
        console.log("Sending request to suggest data for:", targetColumn);
        fetch('/suggest_sample_data', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ target_column: targetColumn })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            console.log("Received response for:", targetColumn);
            return response.json();
        })
        .then(data => {
            if (data.success) {
                console.log("Received AI suggested data:", data.sample_data);
                
                // Update the AI data cell in the table
                if (matchingRow) {
                    // Get the AI data cell directly by ID instead of using querySelector
                    const aiDataCellId = `aiCell-${targetColumn.replace(/ /g, '_')}`;
                    const aiDataCell = document.getElementById(aiDataCellId);
                    
                    if (aiDataCell) {
                        console.log("Updating AI data cell with ID:", aiDataCellId);
                        
                        // Create new AI data display with radio button
                        let aiDataHtml = `
                            <div class="form-check">
                                <input class="form-check-input data-radio" type="radio" 
                                       name="dataSource-${targetColumn.replace(/ /g, '_')}"
                                       id="aiRadio-${targetColumn.replace(/ /g, '_')}" 
                                       data-target="${targetColumn}" data-source="ai">
                                <label class="form-check-label" for="aiRadio-${targetColumn.replace(/ /g, '_')}">
                                    <div class="sample-data">`;
                        
                        const displayCount = Math.min(data.sample_data.length, 3);
                        
                        for (let i = 0; i < displayCount; i++) {
                            aiDataHtml += `<div class="sample-item">${data.sample_data[i]}</div>`;
                        }
                        
                        if (data.sample_data.length > 3) {
                            aiDataHtml += `<div class="sample-more">+ ${data.sample_data.length - 3} more</div>`;
                        }
                        
                        aiDataHtml += `</div>
                                </label>
                            </div>`;
                        
                        // Update the AI data cell HTML
                        aiDataCell.innerHTML = aiDataHtml;
                        console.log("Updated AI data cell HTML");
                        
                        // Enable the radio button
                        const aiRadio = document.getElementById(`aiRadio-${targetColumn.replace(/ /g, '_')}`);
                        if (aiRadio) {
                            aiRadio.disabled = false;
                            console.log("Enabled AI radio button");
                        }
                        
                        // Store AI data for export
                        aiSuggestedData[targetColumn] = data.sample_data;
                        
                        // If this was a "No match found" row, we need to update the UI to show the Select Data button
                        if (isNoMatch) {
                            const actionsCell = matchingRow.querySelector('td:nth-child(6)');
                            if (actionsCell) {
                                const btnGroup = actionsCell.querySelector('.btn-group');
                                
                                if (btnGroup) {
                                    // Find the re-match button
                                    const reMatchBtn = btnGroup.querySelector('.re-match-btn');
                                    
                                    if (reMatchBtn) {
                                        // Create a new select data button
                                        const selectDataBtn = document.createElement('button');
                                        selectDataBtn.className = 'btn btn-sm btn-outline-secondary select-data-btn';
                                        selectDataBtn.setAttribute('data-target', targetColumn);
                                        selectDataBtn.textContent = 'Select Data';
                                        
                                        // Add event listener to the new button
                                        selectDataBtn.addEventListener('click', function() {
                                            currentTarget = this.getAttribute('data-target');
                                            currentHeader = this.getAttribute('data-header') || 'Custom Selection';
                                            
                                            // Set the header name in the modal
                                            document.getElementById('selectedHeaderName').textContent = currentHeader;
                                            
                                            // Show the modal
                                            const modal = new bootstrap.Modal(document.getElementById('dataSelectionModal'));
                                            modal.show();
                                            
                                            // Load Excel data for the selection modal
                                            loadSelectionExcelData();
                                        });
                                        
                                        // Replace the re-match button with the select data button
                                        btnGroup.replaceChild(selectDataBtn, reMatchBtn);
                                    }
                                }
                            }
                        }
                    }
                }
                
                // Show success message without blocking UI update
                setTimeout(() => {
                    alert('AI data generated successfully!');
                }, 100);
            } else {
                // Show error
                alert(data.error || 'An error occurred while suggesting data.');
            }
        })
        .catch(error => {
            alert('An error occurred: ' + error.message);
        })
        .finally(() => {
            // Reset button
            button.innerHTML = originalHtml;
            button.disabled = false;
        });
    });
    
    // Use event delegation for suggest header buttons
    document.addEventListener('click', function(e) {
        // Check if the clicked element is a suggest header button or a child of it
        const button = e.target.closest('.suggest-header-btn');
        if (!button) return; // Not a suggest header button
        
        // Skip disabled buttons
        if (button.classList.contains('disabled') || button.hasAttribute('disabled')) {
            return;
        }
        
        const targetColumn = button.getAttribute('data-target');
        console.log("Suggesting header for column:", targetColumn);
        
        // Disable button and show spinner
        button.disabled = true;
        const originalHtml = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Suggesting...';
        
        // Send request to suggest header for this target
        fetch('/suggest_header', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ target_column: targetColumn })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            if (data.success) {
                console.log("Received AI suggested header:", data.suggested_header);
                
                // Update the AI header cell in the table
                const aiHeaderCellId = `aiHeader-${targetColumn.replace(/ /g, '_')}`;
                const aiHeaderCell = document.getElementById(aiHeaderCellId);
                
                if (aiHeaderCell) {
                    // Update the cell with the suggested header
                    aiHeaderCell.innerHTML = `<span class="text-primary">${data.suggested_header}</span>`;
                    
                    // Show success message
                    setTimeout(() => {
                        alert('AI header suggested successfully!');
                    }, 100);
                } else {
                    alert('Error: Could not find the AI header cell to update.');
                }
            } else {
                // Show error
                alert(data.error || 'An error occurred while suggesting header.');
            }
        })
        .catch(error => {
            alert('An error occurred: ' + error.message);
        })
        .finally(() => {
            // Reset button
            button.innerHTML = originalHtml;
            button.disabled = false;
        });
    });
    
    // Use event delegation for revert data buttons
    document.addEventListener('click', function(e) {
        // Check if the clicked element is a revert data button or a child of it
        const button = e.target.closest('.revert-data-btn');
        if (!button) return; // Not a revert data button
        
        handleRevertClick.call(button);
    });
    
    // This function is no longer used as we've removed the revert button functionality
    // Keeping it as a placeholder in case we need to restore it later
    function handleRevertClick() {
        // Functionality removed as per user request
    }
    
    // Handle data selection buttons
    selectDataButtons.forEach(button => {
        button.addEventListener('click', function() {
            currentTarget = this.getAttribute('data-target');
            currentHeader = this.getAttribute('data-header');
            
            // Set the header name in the modal
            document.getElementById('selectedHeaderName').textContent = currentHeader;
            
            // Show the modal
            const modal = new bootstrap.Modal(document.getElementById('dataSelectionModal'));
            modal.show();
            
            // Load Excel data for the selection modal
            loadSelectionExcelData();
        });
    });
    
    // Handle confirm data selection button
    document.getElementById('confirmDataSelection').addEventListener('click', function() {
        if (selectedCells.length === 0) {
            alert('Please select at least one cell.');
            return;
        }
        
        // Get the selected data
        const selectedData = selectedCells.map(cell => cell.textContent.trim()).filter(text => text);
        
        // Store the selected data for later use
        console.log("Selected data:", selectedData);
        console.log("Current target:", currentTarget);
        
        // Store the cell coordinates for this target column
        const cellCoordinates = [];
        selectedCells.forEach(cell => {
            // Get the sheet name
            const sheetName = currentSheetName || 'Sheet1';
            
            // Get the cell coordinates in Excel format (e.g., A1)
            const rowIndex = cell.parentElement.rowIndex;
            const cellIndex = Array.from(cell.parentElement.cells).indexOf(cell);
            
            // Convert to Excel-style coordinates (A1 notation)
            const colLetter = getColumnLetter(cellIndex - 1); // -1 to account for row number column
            const cellCoord = `${colLetter}${rowIndex + 1}`;
            
            cellCoordinates.push([sheetName, cellCoord]);
        });
        
        // Store the cell coordinates for this target column
        selectedCellsByTarget[currentTarget] = cellCoordinates;
        console.log("Stored cell coordinates for", currentTarget, ":", cellCoordinates);
        
    // First, update the UI directly
    // Find all rows in the main results table
    const resultsTable = document.querySelector('.table-striped');
    if (resultsTable) {
        console.log("Found results table");
        const allRows = resultsTable.querySelectorAll('tr');
        console.log("Number of rows found:", allRows.length);
        
        // Log all target column names for debugging
        console.log("All target columns in table:");
        allRows.forEach(row => {
            const targetCell = row.querySelector('td:first-child');
            if (targetCell) {
                console.log(" - " + targetCell.textContent.trim());
            }
        });
        
        // Find the matching row for the current target column
        const matchingRow = Array.from(allRows).find(row => {
            const targetCell = row.querySelector('td:first-child');
            return targetCell && targetCell.textContent.trim() === currentTarget;
        });
        
        console.log("Matching row found:", !!matchingRow);
        
        if (matchingRow) {
            // Check if this is a "No match found" row by looking for the re-match button
            const actionsCell = matchingRow.querySelector('td:nth-child(7)'); // Actions column is the 7th column
            const reMatchBtn = actionsCell ? actionsCell.querySelector('.re-match-btn') : null;
            const isNoMatch = !!reMatchBtn;
            
            console.log("Is 'No match found' row:", isNoMatch);
            
            const sampleDataCell = matchingRow.querySelector('td:nth-child(5)'); // Sample data is the 5th column
            console.log("Sample data cell found:", !!sampleDataCell);
            
            if (sampleDataCell) {
                // Create new sample data display with radio button
                let sampleHtml = `
                    <div class="form-check">
                        <input class="form-check-input data-radio" type="radio" 
                               name="dataSource-${currentTarget.replace(/ /g, '_')}"
                               id="sampleRadio-${currentTarget.replace(/ /g, '_')}" 
                               data-target="${currentTarget}" data-source="sample" checked>
                        <label class="form-check-label" for="sampleRadio-${currentTarget.replace(/ /g, '_')}">
                            <div class="sample-data">`;
                
                const displayCount = Math.min(selectedData.length, 3);
                
                for (let i = 0; i < displayCount; i++) {
                    sampleHtml += `<div class="sample-item">${selectedData[i]}</div>`;
                }
                
                if (selectedData.length > 3) {
                    sampleHtml += `<div class="sample-more">+ ${selectedData.length - 3} more</div>`;
                }
                
                sampleHtml += `</div>
                        </label>
                    </div>`;
                
                sampleDataCell.innerHTML = sampleHtml;
                console.log("Updated sample data cell HTML");
                
                // If this was a "No match found" row, we need to update the UI to show the Select Data button
                if (isNoMatch) {
                    // Get the actions cell
                    const actionsCell = matchingRow.querySelector('td:nth-child(7)'); // Actions column is the 7th column
                    if (actionsCell) {
                        const btnGroup = actionsCell.querySelector('.btn-group');
                        if (btnGroup) {
                            // Find the re-match button
                            const reMatchBtn = btnGroup.querySelector('.re-match-btn');
                            if (reMatchBtn) {
                                // Create a new select data button
                                const selectDataBtn = document.createElement('button');
                                selectDataBtn.className = 'btn btn-sm btn-outline-secondary select-data-btn';
                                selectDataBtn.setAttribute('data-target', currentTarget);
                                selectDataBtn.textContent = 'Select Data';
                                
                                // Add event listener to the new button
                                selectDataBtn.addEventListener('click', function() {
                                    currentTarget = this.getAttribute('data-target');
                                    currentHeader = this.getAttribute('data-header') || 'Custom Selection';
                                    
                                    // Set the header name in the modal
                                    document.getElementById('selectedHeaderName').textContent = currentHeader;
                                    
                                    // Show the modal
                                    const modal = new bootstrap.Modal(document.getElementById('dataSelectionModal'));
                                    modal.show();
                                    
                                    // Load Excel data for the selection modal
                                    loadSelectionExcelData();
                                });
                                
                                // Replace the re-match button with the select data button
                                btnGroup.replaceChild(selectDataBtn, reMatchBtn);
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Then send the data to the server
    fetch('/update_sample_data', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            target_column: currentTarget,
            selected_data: selectedData
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Close the modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('dataSelectionModal'));
            modal.hide();
            
            // If this is a "No match found" row, we need to update the UI to show the Select Data button
            if (data.has_match === false) {
                console.log("Server confirmed this is a 'No match found' row");
                
                // Find the matching row again to ensure we have the latest DOM reference
                const allRows = document.querySelectorAll('tr');
                const matchingRow = Array.from(allRows).find(row => {
                    const targetCell = row.querySelector('td:first-child');
                    return targetCell && targetCell.textContent.trim() === data.target_column;
                });
                
                if (matchingRow) {
                    // Get the actions cell
                    const actionsCell = matchingRow.querySelector('td:nth-child(7)'); // Actions column is the 7th column
                    if (actionsCell) {
                        const btnGroup = actionsCell.querySelector('.btn-group');
                        if (btnGroup) {
                            // Find the re-match button
                            const reMatchBtn = btnGroup.querySelector('.re-match-btn');
                            if (reMatchBtn) {
                                // Create a new select data button
                                const selectDataBtn = document.createElement('button');
                                selectDataBtn.className = 'btn btn-sm btn-outline-secondary select-data-btn';
                                selectDataBtn.setAttribute('data-target', data.target_column);
                                selectDataBtn.textContent = 'Select Data';
                                
                                // Add event listener to the new button
                                selectDataBtn.addEventListener('click', function() {
                                    currentTarget = this.getAttribute('data-target');
                                    currentHeader = this.getAttribute('data-header') || 'Custom Selection';
                                    
                                    // Set the header name in the modal
                                    document.getElementById('selectedHeaderName').textContent = currentHeader;
                                    
                                    // Show the modal
                                    const modal = new bootstrap.Modal(document.getElementById('dataSelectionModal'));
                                    modal.show();
                                    
                                    // Load Excel data for the selection modal
                                    loadSelectionExcelData();
                                });
                                
                                // Replace the re-match button with the select data button
                                btnGroup.replaceChild(selectDataBtn, reMatchBtn);
                            }
                        }
                    }
                }
            }
            
            // Update the CSV preview if the CSV tab is active
            if (document.getElementById('csv-content').classList.contains('active')) {
                console.log("CSV tab is active, regenerating preview after data selection update");
                generateCsvPreview();
            }
            
            // Show success message
            alert('Data selection updated successfully!');
        } else {
            alert(data.error || 'An error occurred while updating sample data.');
        }
    })
    .catch(error => {
        alert('An error occurred: ' + error.message);
    });
    });
    
    // Handle re-analyzing all matches
    if (reAnalyzeAllBtn) {
        reAnalyzeAllBtn.addEventListener('click', triggerReAnalyze);
    }
    
    // Handle export to CSV button
    const exportCsvBtn = document.querySelector('a[href="/export_csv"]');
    if (exportCsvBtn) {
        exportCsvBtn.addEventListener('click', function(e) {
            e.preventDefault();
            
            // Collect radio button selections
            const exportSelections = {};
            const dataRadios = document.querySelectorAll('.data-radio:checked');
            
            // Process selected radio buttons
            dataRadios.forEach(radio => {
                const targetColumn = radio.getAttribute('data-target');
                const dataSource = radio.getAttribute('data-source');
                exportSelections[targetColumn] = dataSource;
            });
            
            // Send request to export CSV with selections
            fetch('/export_csv', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    export_selections: exportSelections,
                    ai_data: aiSuggestedData
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Create a blob from the CSV content
                    const blob = new Blob([data.csv_content], { type: 'text/csv' });
                    const url = window.URL.createObjectURL(blob);
                    
                    // Create a temporary link and trigger download
                    const a = document.createElement('a');
                    a.style.display = 'none';
                    a.href = url;
                    a.download = 'header_matching_results.csv';
                    
                    // Append to the document and trigger click
                    document.body.appendChild(a);
                    a.click();
                    
                    // Clean up
                    window.URL.revokeObjectURL(url);
                } else {
                    alert(data.error || 'An error occurred while exporting CSV.');
                }
            })
            .catch(error => {
                alert('An error occurred: ' + error.message);
            });
        });
    }
    
    // Log the selectedCellsByTarget object for debugging
    console.log("selectedCellsByTarget:", selectedCellsByTarget);
    
    // Handle export mapping button
    const exportMappingBtn = document.getElementById('exportMapping');
    const exportMappingSpinner = document.getElementById('exportMappingSpinner');
    
    // Function to map cells in the Excel UI
    function mapCellsInExcelUI(targetColumn) {
        // Show a modal to select cells in the Excel UI
        const modal = new bootstrap.Modal(document.getElementById('dataSelectionModal'));
        
        // Set the header name in the modal
        document.getElementById('selectedHeaderName').textContent = targetColumn;
        
        // Show the modal
        modal.show();
        
        // Load Excel data for the selection modal
        loadSelectionExcelData();
        
        // Override the confirm button to store cell coordinates
        const confirmBtn = document.getElementById('confirmDataSelection');
        const originalConfirmHandler = confirmBtn.onclick;
        
        confirmBtn.onclick = function() {
            if (selectedCells.length === 0) {
                alert('Please select at least one cell.');
                return;
            }
            
            // Get the selected cells with their coordinates
            const cellCoordinates = [];
            
            selectedCells.forEach(cell => {
                // Get the sheet name
                const sheetName = currentSheetName || 'Sheet1';
                
                // Get the cell coordinates in Excel format (e.g., A1)
                const rowIndex = cell.parentElement.rowIndex;
                const cellIndex = Array.from(cell.parentElement.cells).indexOf(cell);
                
                // Convert to Excel-style coordinates (A1 notation)
                const colLetter = getColumnLetter(cellIndex - 1); // -1 to account for row number column
                const cellCoord = `${colLetter}${rowIndex + 1}`;
                
                cellCoordinates.push([sheetName, cellCoord]);
            });
            
            // Store the cell coordinates for this target column
            selectedCellsByTarget[targetColumn] = cellCoordinates;
            
            // Close the modal
            modal.hide();
            
            // Show success message
            alert(`Cell coordinates mapped for ${targetColumn}`);
        };
    }
    
    // Function to convert column index to letter (A, B, C, ..., Z, AA, AB, ...)
    function getColumnLetter(colIdx) {
        let result = "";
        while (true) {
            const quotient = Math.floor(colIdx / 26);
            const remainder = colIdx % 26;
            result = String.fromCharCode(65 + remainder) + result;
            if (quotient === 0) {
                break;
            }
            colIdx = quotient - 1;
        }
        return result;
    }
    
    // Map Cells buttons have been removed as per requirements
    
    // Add an Auto Map button next to the Export Mapping button
    if (exportMappingBtn) {
        // Create the Auto Map button
        const autoMapBtn = document.createElement('button');
        autoMapBtn.id = 'autoMapBtn';
        autoMapBtn.className = 'btn btn-warning';
        autoMapBtn.textContent = 'Auto Map';
        
        // Create a spinner for the Auto Map button
        const autoMapSpinner = document.createElement('div');
        autoMapSpinner.id = 'autoMapSpinner';
        autoMapSpinner.className = 'mt-2 d-none';
        autoMapSpinner.innerHTML = `
            <div class="spinner-border spinner-border-sm text-warning" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <span class="ms-2">Auto mapping...</span>
        `;
        
        // Insert the Auto Map button and spinner before the Export Mapping button
        exportMappingBtn.parentNode.insertBefore(autoMapBtn, exportMappingBtn);
        exportMappingBtn.parentNode.insertBefore(autoMapSpinner, exportMappingBtn);
        
        // Add event listener to the Auto Map button
        autoMapBtn.addEventListener('click', function() {
            // Show spinner
            autoMapSpinner.classList.remove('d-none');
            autoMapBtn.disabled = true;
            
            // Collect radio button selections
            const exportSelections = {};
            const dataRadios = document.querySelectorAll('.data-radio:checked');
            
            // Process selected radio buttons
            dataRadios.forEach(radio => {
                const targetColumn = radio.getAttribute('data-target');
                const dataSource = radio.getAttribute('data-source');
                exportSelections[targetColumn] = dataSource;
            });
            
            // Get all target columns from the table
            const allTargetColumns = [];
            const targetCells = document.querySelectorAll('.table-striped tbody tr td:first-child');
            targetCells.forEach(cell => {
                allTargetColumns.push(cell.textContent.trim());
            });
            
            console.log("All target columns for auto mapping:", allTargetColumns);
            
            // Send request to export mapping with selections and auto_mapping=true
            fetch('/export_mapping', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    export_selections: exportSelections,
                    ai_data: aiSuggestedData,
                    selected_cells: selectedCellsByTarget,
                    auto_mapping: true,
                    all_target_columns: allTargetColumns
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Create a blob from the JSON content
                    const jsonStr = JSON.stringify(data.mapping, null, 2);
                    const blob = new Blob([jsonStr], { type: 'application/json' });
                    const url = window.URL.createObjectURL(blob);
                    
                    // Create a temporary link and trigger download
                    const a = document.createElement('a');
                    a.style.display = 'none';
                    a.href = url;
                    a.download = 'excel_mapping.json';
                    
                    // Append to the document and trigger click
                    document.body.appendChild(a);
                    a.click();
                    
                    // Clean up
                    window.URL.revokeObjectURL(url);
                    document.body.removeChild(a);
                    
                    // Show success message
                    alert('Auto mapping completed successfully! The mapping has been exported as JSON.');
                } else {
                    alert(data.error || 'An error occurred while auto mapping.');
                }
            })
            .catch(error => {
                alert('An error occurred: ' + error.message);
            })
            .finally(() => {
                // Hide spinner
                autoMapSpinner.classList.add('d-none');
                autoMapBtn.disabled = false;
            });
        });
        
        // Add event listener to the Export Mapping button
        exportMappingBtn.addEventListener('click', function() {
            // Show spinner
            exportMappingSpinner.classList.remove('d-none');
            exportMappingBtn.disabled = true;
            
            // Collect radio button selections
            const exportSelections = {};
            const dataRadios = document.querySelectorAll('.data-radio:checked');
            
            // Process selected radio buttons
            dataRadios.forEach(radio => {
                const targetColumn = radio.getAttribute('data-target');
                const dataSource = radio.getAttribute('data-source');
                exportSelections[targetColumn] = dataSource;
            });
            
            // Get all target columns from the table
            const allTargetColumns = [];
            const targetCells = document.querySelectorAll('.table-striped tbody tr td:first-child');
            targetCells.forEach(cell => {
                allTargetColumns.push(cell.textContent.trim());
            });
            
            // Send request to export mapping with selections
            fetch('/export_mapping', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    export_selections: exportSelections,
                    ai_data: aiSuggestedData,
                    selected_cells: selectedCellsByTarget,
                    auto_mapping: false,
                    all_target_columns: allTargetColumns
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Create a blob from the JSON content
                    const jsonStr = JSON.stringify(data.mapping, null, 2);
                    const blob = new Blob([jsonStr], { type: 'application/json' });
                    const url = window.URL.createObjectURL(blob);
                    
                    // Create a temporary link and trigger download
                    const a = document.createElement('a');
                    a.style.display = 'none';
                    a.href = url;
                    a.download = 'excel_mapping.json';
                    
                    // Append to the document and trigger click
                    document.body.appendChild(a);
                    a.click();
                    
                    // Clean up
                    window.URL.revokeObjectURL(url);
                    document.body.removeChild(a);
                    
                    // Show success message
                    alert('Mapping exported successfully with Excel cell coordinates!');
                } else {
                    alert(data.error || 'An error occurred while exporting mapping.');
                }
            })
            .catch(error => {
                alert('An error occurred: ' + error.message);
            })
            .finally(() => {
                // Hide spinner
                exportMappingSpinner.classList.add('d-none');
                exportMappingBtn.disabled = false;
            });
        });
    }
    
    // Handle load more button
    if (loadMoreButton) {
        loadMoreButton.addEventListener('click', function() {
            loadMoreExcelRows();
        });
    }
    
    // Function to load Excel preview data
    function loadExcelPreview(sheetName = '', startRow = 0) {
        // Show loading indicator
        excelLoadingIndicator.classList.remove('d-none');
        
        // Hide load more button while loading
        loadMoreContainer.classList.add('d-none');
        
        // Build URL with query parameters
        let url = `/get_excel_preview?rows=${rowsPerPage}&start=${startRow}`;
        if (sheetName) {
            url += `&sheet=${encodeURIComponent(sheetName)}`;
        }
        
        // Fetch Excel preview data
        fetch(url)
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    // Show error message
                    excelSheetContent.innerHTML = `
                        <div class="alert alert-danger">
                            Error loading Excel data: ${data.error}
                        </div>
                    `;
                    return;
                }
                
                // Update state variables
                currentSheetName = data.sheet_name;
                sheetNames = data.sheet_names;
                currentStartRow = data.start_row;
                hasMoreRows = data.has_more;
                totalRows = data.total_rows;
                totalCols = data.total_cols;
                
                // If this is the first load, create the sheet tabs
                if (startRow === 0 && excelSheetTabs.children.length === 0) {
                    createSheetTabs(data.sheet_names, data.sheet_name);
                }
                
                // If this is the first load or a new sheet, create the table
                if (startRow === 0) {
                    createExcelTable(data.sheet_name, data.data, data.row_numbers);
                } else {
                    // Otherwise, append rows to the existing table
                    appendExcelRows(data.sheet_name, data.data, data.row_numbers);
                }
                
                // Update the status text
                updateExcelStatus(data.sheet_name, data.end_row, data.total_rows, data.total_cols);
                
                // Show/hide load more button
                if (data.has_more) {
                    loadMoreContainer.classList.remove('d-none');
                } else {
                    loadMoreContainer.classList.add('d-none');
                }
            })
            .catch(error => {
                console.error('Error loading Excel preview:', error);
                excelSheetContent.innerHTML = `
                    <div class="alert alert-danger">
                        Error loading Excel data: ${error.message}
                    </div>
                `;
            })
            .finally(() => {
                // Hide loading indicator
                excelLoadingIndicator.classList.add('d-none');
            });
    }
    
    // Function to create sheet tabs
    function createSheetTabs(sheetNames, activeSheet) {
        // Clear existing tabs
        excelSheetTabs.innerHTML = '';
        
        // Create a tab for each sheet
        sheetNames.forEach((sheetName, index) => {
            const isActive = sheetName === activeSheet;
            
            const li = document.createElement('li');
            li.className = 'nav-item';
            li.setAttribute('role', 'presentation');
            
            const button = document.createElement('button');
            button.className = `nav-link ${isActive ? 'active' : ''}`;
            button.setAttribute('id', `sheet-${index}-tab`);
            button.setAttribute('data-bs-toggle', 'tab');
            button.setAttribute('data-bs-target', `#sheet-${index}`);
            button.setAttribute('type', 'button');
            button.setAttribute('role', 'tab');
            button.setAttribute('aria-controls', `sheet-${index}`);
            button.setAttribute('aria-selected', isActive ? 'true' : 'false');
            button.setAttribute('data-sheet', sheetName);
            button.textContent = sheetName;
            
            // Add click event to load sheet data
            button.addEventListener('click', function() {
                const sheetName = this.getAttribute('data-sheet');
                loadExcelPreview(sheetName, 0);
            });
            
            li.appendChild(button);
            excelSheetTabs.appendChild(li);
        });
    }
    
    // Function to create Excel table
    function createExcelTable(sheetName, data, rowNumbers) {
        // Create a div for this sheet
        const sheetIndex = sheetNames.indexOf(sheetName);
        const sheetId = `sheet-${sheetIndex}`;
        
        // Create or get the tab pane
        let tabPane = document.getElementById(sheetId);
        if (!tabPane) {
            tabPane = document.createElement('div');
            tabPane.className = 'tab-pane fade';
            tabPane.id = sheetId;
            tabPane.setAttribute('role', 'tabpanel');
            tabPane.setAttribute('aria-labelledby', `${sheetId}-tab`);
            
            // If this is the active sheet, add the active class
            if (sheetName === currentSheetName) {
                tabPane.classList.add('show', 'active');
            }
            
            excelSheetContent.appendChild(tabPane);
        } else {
            // Clear existing content
            tabPane.innerHTML = '';
        }
        
        // Create the grid container
        const gridContainer = document.createElement('div');
        gridContainer.className = 'excel-grid-container';
        
        // Create the grid
        const grid = document.createElement('div');
        grid.className = 'excel-grid';
        grid.id = `excel-grid-${sheetIndex}`;
        grid.setAttribute('data-sheet', sheetName);
        
        // Create the table
        const table = document.createElement('table');
        table.className = 'table table-sm table-bordered excel-table';
        
        // Create the tbody
        const tbody = document.createElement('tbody');
        
        // Add rows to the table
        data.forEach((rowData, rowIndex) => {
            const tr = document.createElement('tr');
            
            // Add row number cell
            const rowNumberCell = document.createElement('td');
            rowNumberCell.className = 'row-number';
            rowNumberCell.textContent = rowNumbers[rowIndex];
            tr.appendChild(rowNumberCell);
            
            // Add data cells
            rowData.forEach(cellData => {
                const td = document.createElement('td');
                td.textContent = cellData;
                tr.appendChild(td);
            });
            
            tbody.appendChild(tr);
        });
        
        // Assemble the table
        table.appendChild(tbody);
        grid.appendChild(table);
        gridContainer.appendChild(grid);
        
        // Create status text
        const statusText = document.createElement('div');
        statusText.className = 'text-muted small mt-2';
        statusText.id = `status-${sheetIndex}`;
        
        // Add to tab pane
        tabPane.appendChild(gridContainer);
        tabPane.appendChild(statusText);
    }
    
    // Function to append rows to an existing Excel table
    function appendExcelRows(sheetName, data, rowNumbers) {
        const sheetIndex = sheetNames.indexOf(sheetName);
        const tableBody = document.querySelector(`#excel-grid-${sheetIndex} table tbody`);
        
        if (!tableBody) return;
        
        // Add rows to the table
        data.forEach((rowData, rowIndex) => {
            const tr = document.createElement('tr');
            
            // Add row number cell
            const rowNumberCell = document.createElement('td');
            rowNumberCell.className = 'row-number';
            rowNumberCell.textContent = rowNumbers[rowIndex];
            tr.appendChild(rowNumberCell);
            
            // Add data cells
            rowData.forEach(cellData => {
                const td = document.createElement('td');
                td.textContent = cellData;
                tr.appendChild(td);
            });
            
            tableBody.appendChild(tr);
        });
    }
    
    // Function to update Excel status text
    function updateExcelStatus(sheetName, loadedRows, totalRows, totalCols) {
        const sheetIndex = sheetNames.indexOf(sheetName);
        const statusElement = document.getElementById(`status-${sheetIndex}`);
        
        if (statusElement) {
            statusElement.textContent = `Showing ${loadedRows} rows of ${totalRows} total rows and ${totalCols} columns.`;
        }
    }
    
    // Function to generate CSV preview based on current radio button selections
    function generateCsvPreview() {
        console.log("Generating CSV preview with completely new implementation");
        
        // Show loading indicator
        csvLoadingIndicator.classList.remove('d-none');
        
        // Clear any existing content
        csvPreviewContainer.innerHTML = '';
        
        // Collect radio button selections
        const exportSelections = {};
        const dataRadios = document.querySelectorAll('.data-radio:checked');
        
        // Process selected radio buttons
        dataRadios.forEach(radio => {
            const targetColumn = radio.getAttribute('data-target');
            const dataSource = radio.getAttribute('data-source');
            exportSelections[targetColumn] = dataSource;
        });
        
        // Get all target columns from the table
        const targetColumns = [];
        const targetCells = document.querySelectorAll('.table-striped tbody tr td:first-child');
        targetCells.forEach(cell => {
            targetColumns.push(cell.textContent.trim());
        });
        
        // Always fetch fresh data from the server first, then generate the preview
        // This ensures we're using the most up-to-date data after AI edits
        fetch('/get_all_sample_data', {
            method: 'GET'
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Reset local cache and use the server data
                originalSampleData = {}; // Clear the cached data
                
                // Update originalSampleData with complete datasets from server
                Object.keys(data.sample_data).forEach(targetColumn => {
                    originalSampleData[targetColumn] = [...data.sample_data[targetColumn]];
                });
                
                console.log("Fetched complete sample data from server:", originalSampleData);
                
                // Proceed with preview generation
                generatePreviewContent(targetColumns, exportSelections);
            } else {
                csvPreviewContainer.innerHTML = '<div class="alert alert-danger">Failed to load updated data</div>';
                csvLoadingIndicator.classList.add('d-none');
            }
        })
        .catch(error => {
            console.error("Error fetching complete sample data:", error);
            csvPreviewContainer.innerHTML = '<div class="alert alert-danger">Error loading data: ' + error.message + '</div>';
            csvLoadingIndicator.classList.add('d-none');
        });
    }
    
    // New function to handle the preview generation after fetching fresh data
    function generatePreviewContent(targetColumns, exportSelections) {
        // Clear any existing content again to ensure we don't have multiple tables
        csvPreviewContainer.innerHTML = '';
        
        // Collect data for each target column based on selection
        const columnData = {};
        
        // Use the updated originalSampleData which was just fetched
        targetColumns.forEach(targetColumn => {
            const dataSource = exportSelections[targetColumn] || 'sample'; // Default to sample if not specified
            
            if (dataSource === 'ai') {
                // Use AI suggested data
                columnData[targetColumn] = aiSuggestedData[targetColumn] || [];
                console.log(`Using AI data for ${targetColumn}:`, columnData[targetColumn]);
            } else {
                // Use sample data from our freshly fetched data
                if (originalSampleData[targetColumn] && originalSampleData[targetColumn].length > 0) {
                    columnData[targetColumn] = [...originalSampleData[targetColumn]]; // Use a copy to avoid modifying the original
                } else {
                    columnData[targetColumn] = [];
                }
                console.log(`Using sample data for ${targetColumn}:`, columnData[targetColumn]);
            }
        });
        
        // Determine the maximum number of rows
        let maxRows = 0;
        Object.values(columnData).forEach(data => {
            maxRows = Math.max(maxRows, data.length);
        });
        
        // Debug: Log the collected data
        console.log("CSV Preview - Target Columns:", targetColumns);
        console.log("CSV Preview - Export Selections:", exportSelections);
        console.log("CSV Preview - Column Data:", columnData);
        console.log("CSV Preview - Max Rows:", maxRows);
        
        // Create a simple HTML structure for the CSV preview
        const previewDiv = document.createElement('div');
        previewDiv.style.overflowX = 'auto';
        previewDiv.style.maxHeight = '500px';
        previewDiv.style.overflowY = 'auto';
        
        // Create a simple HTML table with basic styling
        const tableHTML = `
            <table style="border-collapse: collapse; width: auto; margin-bottom: 0;">
                <thead>
                    <tr>
                        <th style="background-color: #f8f9fa; font-weight: bold; text-align: center; width: 40px; min-width: 40px; max-width: 40px; position: sticky; left: 0; z-index: 1; border: 1px solid #dee2e6; padding: 8px;">#</th>
                        ${targetColumns.map(column => `
                            <th style="background-color: #b8daff; color: #000; font-weight: bold; text-align: center; padding: 10px 15px; border: 1px solid #dee2e6; min-width: 150px; max-width: 250px; white-space: normal; word-wrap: break-word;">${column}</th>
                        `).join('')}
                    </tr>
                </thead>
                <tbody>
                    ${Array.from({ length: maxRows }, (_, i) => `
                        <tr>
                            <td style="background-color: #f8f9fa; font-weight: bold; text-align: center; width: 40px; min-width: 40px; max-width: 40px; position: sticky; left: 0; z-index: 1; border: 1px solid #dee2e6; padding: 8px;">${i + 1}</td>
                            ${targetColumns.map(column => `
                                <td style="padding: 6px 10px; border: 1px solid #dee2e6; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 150px;">${i < columnData[column].length ? columnData[column][i] : ''}</td>
                            `).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
        
        previewDiv.innerHTML = tableHTML;
        
        // Add the preview div to the container
        csvPreviewContainer.appendChild(previewDiv);
        
        // Add a status message
        const statusText = document.createElement('div');
        statusText.className = 'text-muted small mt-2';
        
        if (maxRows === 0) {
            statusText.textContent = 'No data available for CSV preview. Please select data for at least one column.';
            statusText.className = 'alert alert-warning mt-2';
        } else {
            statusText.textContent = `CSV preview showing ${targetColumns.length} columns and ${maxRows} rows based on your current selections.`;
        }
        
        csvPreviewContainer.appendChild(statusText);
        
        // Hide loading indicator
        csvLoadingIndicator.classList.add('d-none');
    }
    
    // Function to load more Excel rows
    function loadMoreExcelRows() {
        // Show spinner
        loadMoreSpinner.classList.remove('d-none');
        loadMoreButton.disabled = true;
        
        // Calculate next start row
        const nextStartRow = currentStartRow + rowsPerPage;
        
        // Load more rows
        loadExcelPreview(currentSheetName, nextStartRow);
        
        // Hide spinner
        loadMoreSpinner.classList.add('d-none');
        loadMoreButton.disabled = false;
    }
    
    // Function to load Excel data for the selection modal
    function loadSelectionExcelData() {
        const selectionGrid = document.getElementById('selectionExcelGrid');
        const loadingIndicator = document.getElementById('selectionLoadingIndicator');
        
        // Show loading indicator
        loadingIndicator.classList.remove('d-none');
        
        // Fetch Excel preview data for the first sheet
        fetch(`/get_excel_preview?rows=1000`)  // Load up to 1000 rows for better data coverage
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    selectionGrid.innerHTML = `
                        <div class="alert alert-danger">
                            Error loading Excel data: ${data.error}
                        </div>
                    `;
                    return;
                }
                
                // Create the table
                const table = document.createElement('table');
                table.className = 'table table-sm table-bordered excel-table';
                
                // Create the tbody
                const tbody = document.createElement('tbody');
                
                // Add rows to the table
                data.data.forEach((rowData, rowIndex) => {
                    const tr = document.createElement('tr');
                    
                    // Add row number cell
                    const rowNumberCell = document.createElement('td');
                    rowNumberCell.className = 'row-number';
                    rowNumberCell.textContent = data.row_numbers[rowIndex];
                    tr.appendChild(rowNumberCell);
                    
                    // Add data cells
                    rowData.forEach(cellData => {
                        const td = document.createElement('td');
                        td.textContent = cellData;
                        tr.appendChild(td);
                    });
                    
                    tbody.appendChild(tr);
                });
                
                // Assemble the table
                table.appendChild(tbody);
                
                // Clear selection grid and add table
                selectionGrid.innerHTML = '';
                selectionGrid.appendChild(table);
                
                // Make cells selectable
                makeGridSelectable(selectionGrid);
            })
            .catch(error => {
                console.error('Error loading Excel data for selection:', error);
                selectionGrid.innerHTML = `
                    <div class="alert alert-danger">
                        Error loading Excel data: ${error.message}
                    </div>
                `;
            })
            .finally(() => {
                // Hide loading indicator
                loadingIndicator.classList.add('d-none');
            });
    }
    
    // Function to make the Excel grid cells selectable
    function makeGridSelectable(gridElement) {
        // Reset selection variables
        selectionStart = null;
        selectionEnd = null;
        selectedCells = [];
        
        // Clear the selected data preview
        document.getElementById('selectedDataPreview').innerHTML = '<em>No data selected</em>';
        
        // Get all cells in the grid except row number cells
        const cells = gridElement.querySelectorAll('td:not(.row-number)');
        
        // Add selectable class to all cells
        cells.forEach(cell => {
            cell.classList.add('selectable-cell');
            
            // Add mousedown event (start selection)
            cell.addEventListener('mousedown', function(e) {
                e.preventDefault(); // Prevent text selection
                
                // Clear previous selection
                clearSelection();
                
                // Set selection start
                selectionStart = this;
                this.classList.add('selection-start');
                this.classList.add('selected-cell');
                
                // Add this cell to selected cells
                selectedCells = [this];
                updateSelectedDataPreview();
            });
            
            // Add mousemove event (for drag selection, but only when mouse button is down)
            cell.addEventListener('mousemove', function(e) {
                // Only respond to mousemove when mouse button is held down (selectionStart is set)
                if (selectionStart && e.buttons === 1) {
                    // Clear previous selection except start
                    clearSelection(true);
                    
                    // Set selection end
                    selectionEnd = this;
                    
                    // Get all cells in the selection range
                    const range = getCellsInRange(selectionStart, selectionEnd);
                    selectedCells = range;
                    
                    // Highlight selected cells
                    highlightSelectedRange(range);
                    
                    // Update the preview
                    updateSelectedDataPreview();
                }
            });
        });
        
        // Add mouseup event to document (end selection)
        document.addEventListener('mouseup', function() {
            if (selectionStart) {
                // Keep the selection, just remove the mousedown state
                selectionStart.classList.remove('selection-start');
            }
        });
    }
    
    // Function to clear cell selection
    function clearSelection(keepStart = false) {
        // Remove all selection classes
        const cells = document.querySelectorAll('.selected-cell, .selection-top, .selection-right, .selection-bottom, .selection-left');
        cells.forEach(cell => {
            cell.classList.remove('selected-cell', 'selection-top', 'selection-right', 'selection-bottom', 'selection-left');
        });
        
        if (!keepStart && selectionStart) {
            selectionStart.classList.remove('selection-start');
        }
    }
    
    // Function to highlight the selected range with Excel-like borders
    function highlightSelectedRange(cells) {
        if (!cells.length) return;
        
        // First, add the selected-cell class to all cells in the range
        cells.forEach(cell => {
            cell.classList.add('selected-cell');
        });
        
        // Get the boundaries of the selection
        const boundaries = getSelectionBoundaries(cells);
        
        // Add border classes to create the Excel-like selection appearance
        cells.forEach(cell => {
            const rowIndex = cell.parentElement.rowIndex;
            const cellIndex = Array.from(cell.parentElement.cells).indexOf(cell);
            
            // Top border for cells in the first row of selection
            if (rowIndex === boundaries.minRow) {
                cell.classList.add('selection-top');
            }
            
            // Right border for cells in the last column of selection
            if (cellIndex === boundaries.maxCol) {
                cell.classList.add('selection-right');
            }
            
            // Bottom border for cells in the last row of selection
            if (rowIndex === boundaries.maxRow) {
                cell.classList.add('selection-bottom');
            }
            
            // Left border for cells in the first column of selection
            if (cellIndex === boundaries.minCol) {
                cell.classList.add('selection-left');
            }
        });
    }
    
    // Function to get the boundaries of the selection
    function getSelectionBoundaries(cells) {
        let minRow = Infinity;
        let maxRow = -Infinity;
        let minCol = Infinity;
        let maxCol = -Infinity;
        
        cells.forEach(cell => {
            const rowIndex = cell.parentElement.rowIndex;
            const cellIndex = Array.from(cell.parentElement.cells).indexOf(cell);
            
            minRow = Math.min(minRow, rowIndex);
            maxRow = Math.max(maxRow, rowIndex);
            minCol = Math.min(minCol, cellIndex);
            maxCol = Math.max(maxCol, cellIndex);
        });
        
        return { minRow, maxRow, minCol, maxCol };
    }
    
    // Function to get all cells in a range between two cells
    function getCellsInRange(start, end) {
        // Find the row and column of start and end cells
        const startRow = start.parentElement;
        const endRow = end.parentElement;
        
        const startRowIndex = startRow.rowIndex;
        const endRowIndex = endRow.rowIndex;
        
        const startColIndex = Array.from(startRow.cells).indexOf(start);
        const endColIndex = Array.from(endRow.cells).indexOf(end);
        
        // Determine the range boundaries
        const minRowIndex = Math.min(startRowIndex, endRowIndex);
        const maxRowIndex = Math.max(startRowIndex, endRowIndex);
        const minColIndex = Math.min(startColIndex, endColIndex);
        const maxColIndex = Math.max(startColIndex, endColIndex);
        
        // Get all rows in the table
        const table = start.closest('table');
        if (!table) return [];
        
        const rows = Array.from(table.querySelectorAll('tr'));
        
        // Collect cells in the range (excluding row number cells)
        const cellsInRange = [];
        for (let i = minRowIndex; i <= maxRowIndex; i++) {
            const row = rows[i];
            if (row) {
                const rowCells = Array.from(row.cells);
                for (let j = minColIndex; j <= maxColIndex; j++) {
                    if (rowCells[j] && !rowCells[j].classList.contains('row-number')) {
                        cellsInRange.push(rowCells[j]);
                    }
                }
            }
        }
        
        return cellsInRange;
    }
    
    // Function to update the selected data preview
    function updateSelectedDataPreview() {
        const previewElement = document.getElementById('selectedDataPreview');
        
        if (selectedCells.length === 0) {
            previewElement.innerHTML = '<em>No data selected</em>';
            return;
        }
        
        // Get the text content of selected cells
        const selectedData = selectedCells.map(cell => cell.textContent.trim()).filter(text => text);
        
        if (selectedData.length === 0) {
            previewElement.innerHTML = '<em>No data in selected cells</em>';
            return;
        }
        
        // Create preview HTML
        let previewHtml = '';
        const displayCount = Math.min(selectedData.length, 10);
        
        for (let i = 0; i < displayCount; i++) {
            previewHtml += `<div>${selectedData[i]}</div>`;
        }
        
        if (selectedData.length > 10) {
            previewHtml += `<div class="text-muted">(${selectedData.length - 10} more items not shown)</div>`;
        }
        
        previewElement.innerHTML = previewHtml;
    }
    
    function triggerReAnalyze() {
        // Show spinner and disable button
        reAnalyzeSpinner.classList.remove('d-none');
        if (reAnalyzeAllBtn) reAnalyzeAllBtn.disabled = true;
        
        // Send request to re-analyze all matches
        fetch('/re_analyze_all', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Refresh the page to show updated results
                window.location.reload();
            } else {
                // Reset and show error
                reAnalyzeSpinner.classList.add('d-none');
                if (reAnalyzeAllBtn) reAnalyzeAllBtn.disabled = false;
                alert(data.error || 'An error occurred while re-analyzing.');
            }
        })
        .catch(error => {
            reAnalyzeSpinner.classList.add('d-none');
            if (reAnalyzeAllBtn) reAnalyzeAllBtn.disabled = false;
            alert('An error occurred: ' + error.message);
        });
    }
    
    // AI Chatbot functionality
    function setupAIChatbot() {
        const chatContainer = document.getElementById('aiChatbotModal');
        const closeButton = document.querySelector('#aiChatbotModal .btn-close');
        const chatMessages = document.getElementById('chatMessages');
        const chatInput = document.getElementById('chatInput');
        const sendMessageButton = document.getElementById('sendMessageButton');
        const applyChangesButton = document.getElementById('applyChangesButton');
        
        if (chatContainer && closeButton && chatMessages && chatInput && sendMessageButton && applyChangesButton) {
            // Initialize chatbot state
            const chatbotState = {
                messages: [],
                editedCsvData: null,
                pendingChanges: false,
                previousState: null  // Add this to store state when awaiting clarification
            };
            
            // Function to get CSV data for editing
            function captureCsvData() {
                // Reset chatbot state
                chatbotState.messages = [];
                chatbotState.editedCsvData = null;
                chatbotState.pendingChanges = false;
                chatbotState.previousState = null; // Clear previous state
                
                // Reset UI elements
                chatInput.placeholder = "Type your message here...";
                chatInput.classList.remove("clarification-needed");
                sendMessageButton.innerHTML = '<i class="bi bi-send"></i> Send';
                
                // Clear chat messages except the welcome message
                while (chatMessages.children.length > 1) {
                    chatMessages.removeChild(chatMessages.lastChild);
                }
                
                // Collect radio button selections to pass the current export state
                const exportSelections = {};
                const dataRadios = document.querySelectorAll('.data-radio:checked');
                dataRadios.forEach(radio => {
                    const targetColumn = radio.getAttribute('data-target');
                    const dataSource = radio.getAttribute('data-source');
                    exportSelections[targetColumn] = dataSource;
                });
                
                // Show thinking indicator
                showThinkingIndicator();
                
                // Get the CSV data for editing
                fetch('/get_csv_data', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        export_selections: exportSelections
                    })
                })
                .then(response => response.json())
                .then(data => {
                    // Hide thinking indicator
                    hideThinkingIndicator();
                    
                    if (data.success) {
                        chatbotState.editedCsvData = data.csv_data;
                        
                // Log the CSV data for debugging
                if (chatbotState.editedCsvData) {
                    console.log("CSV data received from server:", chatbotState.editedCsvData);
                    if (chatbotState.editedCsvData.data && chatbotState.editedCsvData.data.length > 0) {
                        console.log("First row:", chatbotState.editedCsvData.data[0]);
                    } else {
                        console.warn("CSV data is empty or has no rows");
                    }
                } else {
                    console.error("No CSV data received from server");
                }
                        
                        // Add the first AI message about the CSV data
                        const headerInfo = `The CSV has ${chatbotState.editedCsvData.headers.length} columns: ${chatbotState.editedCsvData.headers.join(', ')}.`;
                        const rowInfo = `There are ${chatbotState.editedCsvData.data.length} rows of data.`;
                        
                        addMessage('ai', `I've loaded the CSV data. ${headerInfo} ${rowInfo} What would you like to edit?`);
                    } else {
                        addMessage('ai', `I encountered an error loading the CSV data: ${data.error || 'Unknown error'}`);
                    }
                })
                .catch(error => {
                    console.error('Error fetching CSV data:', error);
                    addMessage('ai', 'I encountered an error loading the CSV data. Please try again.');
                });
            }
            
            // Function to collect export selections from radio buttons
            function getExportSelections() {
                const exportSelections = {};
                const dataRadios = document.querySelectorAll('.data-radio:checked');
                
                dataRadios.forEach(radio => {
                    const targetColumn = radio.getAttribute('data-target');
                    const dataSource = radio.getAttribute('data-source');
                    exportSelections[targetColumn] = dataSource;
                });
                
                return exportSelections;
            }
            
// Function to get original Excel data
function getOriginalExcelData() {
    // Create a structured object to hold Excel data from the preview
    const excelData = {};
    
    // Get all sheet tabs
    const sheetTabs = document.querySelectorAll('#excelSheetTabs button');
    
    // For each sheet tab, get the corresponding table data
    sheetTabs.forEach(tab => {
        const sheetName = tab.getAttribute('data-sheet');
        const sheetId = tab.getAttribute('data-bs-target').replace('#', '');
        const sheetContent = document.getElementById(sheetId);
        
        if (sheetName && sheetContent) {
            // Find the table in this sheet
            const table = sheetContent.querySelector('table');
            if (table) {
                // Convert table to a DataFrame-like structure
                const rows = Array.from(table.querySelectorAll('tbody tr'));
                const data = [];
                
                rows.forEach(row => {
                    const rowData = Array.from(row.querySelectorAll('td')).slice(1) // Skip row number cell
                        .map(cell => cell.textContent.trim());
                    data.push(rowData);
                });
                
                // Store the data for this sheet
                excelData[sheetName] = {
                    data: data
                };
                
                console.log(`Captured Excel data for sheet ${sheetName}: ${data.length} rows`);
            }
        }
    });
    
    // If we have data, return it
    if (Object.keys(excelData).length > 0) {
        console.log("Returning Excel data for source_data:", excelData);
        return excelData;
    }
    
    console.log("No Excel data found for source_data");
    return null;
}
            
            // Function to send user message
            function sendUserMessage() {
                const message = chatInput.value.trim();
                
                if (!message) {
                    return;
                }
                
                // Add user message to chat
                addMessage('user', message);
                
                // Clear input
                chatInput.value = '';
                
                // Show thinking indicator
                showThinkingIndicator();
                
                // Check if we have a stored state from awaiting clarification
                const requestBody = {
                    message: message,
                    csv_data: chatbotState.editedCsvData,
                    source_data: getOriginalExcelData()
                };
                
                // If we have a stored state from a previous clarification request, include it
                if (chatbotState.previousState && chatbotState.previousState.awaiting_clarification) {
                    requestBody.prev_state = chatbotState.previousState;
                }
                
                // Log the CSV data before sending
                if (requestBody.csv_data) {
                    console.log("CSV data before sending:", requestBody.csv_data);
                    if (requestBody.csv_data.data && requestBody.csv_data.data.length > 0) {
                        console.log("First row:", requestBody.csv_data.data[0]);
                    } else {
                        console.warn("CSV data is empty or has no rows");
                        
                        // Ensure we have at least an empty row
                        if (!requestBody.csv_data.data || requestBody.csv_data.data.length === 0) {
                            requestBody.csv_data.data = [{}];
                            
                            // Add empty values for each header
                            if (requestBody.csv_data.headers && requestBody.csv_data.headers.length > 0) {
                                requestBody.csv_data.headers.forEach(header => {
                                    requestBody.csv_data.data[0][header] = '';
                                });
                            }
                            
                            console.log("Added empty row to CSV data:", requestBody.csv_data.data);
                        }
                    }
                } else {
                    console.error("No CSV data to send");
                }
                
                // Send message to backend
                fetch('/chat_with_csv_editor', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(requestBody)
                })
                .then(response => response.json())
                .then(data => {
                    // Hide thinking indicator
                    hideThinkingIndicator();
                    
                    if (data.success) {
                        // Add AI response to chat
                        addMessage('ai', data.response);
                        
                        // Check if the agent is awaiting clarification
                        if (data.awaiting_clarification && data.state) {
                            // Store the state for the next request
                            chatbotState.previousState = data.state;
                            console.log("Agent is awaiting clarification. State saved for next request.");
                            
                            // Update the UI to indicate awaiting clarification
                            chatInput.placeholder = "Please provide clarification...";
                            chatInput.classList.add("clarification-needed");
                            sendMessageButton.innerHTML = '<i class="bi bi-send"></i> Send Clarification';
                            
                            // Optional: Add visual cue to the last AI message
                            const lastMessage = chatMessages.querySelector('.ai-message:last-of-type .message-content');
                            if (lastMessage) {
                                lastMessage.classList.add("awaiting-clarification");
                            }
                        } else {
                            // Clear previous state since we're not awaiting clarification anymore
                            chatbotState.previousState = null;
                            
                            // Reset UI elements if they were modified for clarification
                            chatInput.placeholder = "Type your message here...";
                            chatInput.classList.remove("clarification-needed");
                            sendMessageButton.innerHTML = '<i class="bi bi-send"></i> Send';
                            
                            // Update CSV data if changed
                            if (data.csv_data && data.csv_data_changed) {
                                console.log("CSV data changed, updating state and UI");
                                chatbotState.editedCsvData = data.csv_data;
                                chatbotState.pendingChanges = true;
                                
                                // Enable apply changes button
                                applyChangesButton.classList.remove('btn-outline-success');
                                applyChangesButton.classList.add('btn-success');
                            }
                        }
                    } else {
                        addMessage('ai', `I encountered an error: ${data.error || 'Unknown error'}`);
                    }
                })
                .catch(error => {
                    console.error('Error chatting with AI:', error);
                    // Add more detailed error logging
                    console.error('Error details:', {
                        name: error.name,
                        message: error.message,
                        stack: error.stack,
                        response: error.response
                    });
                    
                    // Try to get more information if it's a response error
                    if (error.response) {
                        error.response.text().then(text => {
                            console.error('Response text:', text);
                            try {
                                const errorData = JSON.parse(text);
                                console.error('Parsed error data:', errorData);
                                addMessage('ai', `Error: ${errorData.error || 'Unknown error'}`);
                            } catch (e) {
                                console.error('Could not parse error response:', e);
                                addMessage('ai', `Error: ${text || 'Unknown error'}`);
                            }
                        }).catch(e => {
                            console.error('Could not get response text:', e);
                            addMessage('ai', 'I encountered an error processing your request. Please try again.');
                        });
                    } else {
                        hideThinkingIndicator();
                        addMessage('ai', 'I encountered an error processing your request. Please try again.');
                    }
                });
            }
            
            // Wire up event listeners
            // Send message when the button is clicked
            sendMessageButton.addEventListener('click', function() {
                sendUserMessage();
            });
            
            // Send message when Enter key is pressed in the input field
            chatInput.addEventListener('keypress', function(event) {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    sendUserMessage();
                }
            });
            
            // Edit with AI button to open modal and capture data
            const editWithAiButton = document.getElementById('editWithAiButton');
            if (editWithAiButton) {
                // Remove any existing onclick attribute
                editWithAiButton.removeAttribute('onclick');
                
                // Add event listener
                editWithAiButton.addEventListener('click', function() {
                    // Show the modal
                    document.getElementById('aiChatbotModal').classList.add('show'); 
                    document.getElementById('aiChatbotModal').style.display = 'block'; 
                    document.body.classList.add('modal-open');
                    
                    // Add backdrop
                    var backdrop = document.createElement('div');
                    backdrop.className = 'modal-backdrop fade show';
                    document.body.appendChild(backdrop);
                    
                    // Capture CSV data
                    captureCsvData();
                });
            }
            
            // Add a listener for when the modal is closed using the Close button
            const aiModalCloseBtn = document.querySelector('#aiChatbotModal .btn-secondary');
            if (aiModalCloseBtn) {
                aiModalCloseBtn.addEventListener('click', function() {
                    // Update the CSV preview after closing the modal
                    generateCsvPreview();
                });
            }
            
            // Apply changes button
            applyChangesButton.addEventListener('click', function() {
                if (!chatbotState.pendingChanges) {
                    return;
                }
                
                // Show thinking indicator
                showThinkingIndicator();
                
                // Send edited CSV data to backend
                fetch('/update_csv_preview', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        csv_data: chatbotState.editedCsvData
                    })
                })
                .then(response => response.json())
                .then(data => {
                    // Hide thinking indicator
                    hideThinkingIndicator();
                    
                    if (data.success) {
                        // Add success message
                        addMessage('ai', 'I\'ve applied the changes to the CSV preview. You can continue editing or close this dialog to see the updated preview.');
                        
                        // Reset pendingChanges flag
                        chatbotState.pendingChanges = false;
                        
                        // Force refresh of all data and UI
                        originalSampleData = {}; // Clear the cached data

                        // Update the CSV preview first
                        if (typeof generateCsvPreview === 'function') {
                            generateCsvPreview();
                        }
                        
                        // Force refresh the target columns display by fetching new data
                        fetch('/get_all_sample_data')
                            .then(response => response.json())
                            .then(freshData => {
                                if (freshData.success) {
                                    console.log("Fresh data fetched after CSV edit:", freshData);
                                    
                                    // Check for any new columns added by the chatbot
                                    const currentColumns = document.querySelectorAll('.table-striped tbody tr td:first-child');
                                    const existingColumnNames = Array.from(currentColumns).map(col => col.textContent.trim());
                                    const newColumns = Object.keys(freshData.sample_data).filter(col => !existingColumnNames.includes(col));
                                    
                                    if (newColumns.length > 0) {
                                        console.log("New columns detected:", newColumns);
                                        // This requires a page refresh to properly update the UI with new columns
                                        alert("New columns have been added to your data. The page will refresh to show the updated table.");
                                        window.location.reload();
                                    }
                                }
                            })
                            .catch(error => {
                                console.error("Error refreshing data:", error);
                            });
                        
                        // Reset button style
                        applyChangesButton.classList.remove('btn-success');
                        applyChangesButton.classList.add('btn-outline-success');
                    } else {
                        addMessage('ai', `I encountered an error applying the changes: ${data.error || 'Unknown error'}`);
                    }
                })
                .catch(error => {
                    console.error('Error applying changes:', error);
                    hideThinkingIndicator();
                    addMessage('ai', 'I encountered an error applying the changes. Please try again.');
                });
            });
            
            // Function to add a message to the chat
            function addMessage(sender, text) {
                // Create message container
                const messageDiv = document.createElement('div');
                messageDiv.className = `message ${sender}-message`;
                
                // Create message content
                const contentDiv = document.createElement('div');
                contentDiv.className = 'message-content';
                contentDiv.textContent = text;
                
                // Create message timestamp
                const timeSpan = document.createElement('span');
                timeSpan.className = 'message-time';
                timeSpan.textContent = new Date().toLocaleTimeString();
                
                // Assemble message
                messageDiv.appendChild(contentDiv);
                messageDiv.appendChild(timeSpan);
                
                // Add to chat
                chatMessages.appendChild(messageDiv);
                
                // Scroll to bottom
                chatMessages.scrollTop = chatMessages.scrollHeight;
                
                // Add to state
                chatbotState.messages.push({
                    role: sender === 'user' ? 'user' : 'assistant',
                    content: text
                });
            }
            
            // Function to show thinking indicator
            function showThinkingIndicator() {
                // Remove any existing thinking indicators
                hideThinkingIndicator();
                
                // Create thinking container
                const thinkingDiv = document.createElement('div');
                thinkingDiv.className = 'message ai-message';
                thinkingDiv.id = 'thinkingIndicator';
                
                // Create thinking content
                const contentDiv = document.createElement('div');
                contentDiv.className = 'thinking';
                
                // Create thinking dots
                for (let i = 0; i < 3; i++) {
                    const dot = document.createElement('span');
                    dot.className = 'thinking-dot';
                    contentDiv.appendChild(dot);
                }
                
                // Assemble thinking indicator
                thinkingDiv.appendChild(contentDiv);
                
                // Add to chat
                chatMessages.appendChild(thinkingDiv);
                
                // Scroll to bottom
                chatMessages.scrollTop = chatMessages.scrollHeight;
            }
            
            // Function to hide thinking indicator
            function hideThinkingIndicator() {
                const thinkingIndicator = document.getElementById('thinkingIndicator');
                if (thinkingIndicator) {
                    thinkingIndicator.remove();
                }
            }
            
            // Function to close the modal and refresh CSV preview
            function closeModal() {
                // Hide the modal
                document.getElementById('aiChatbotModal').classList.remove('show');
                document.getElementById('aiChatbotModal').style.display = 'none';
                document.body.classList.remove('modal-open');
                
                // Remove backdrop
                var backdrop = document.querySelector('.modal-backdrop');
                if (backdrop) backdrop.remove();
                
                // Clear the cached data to force a fresh fetch
                originalSampleData = {}; // This is now safe since we changed it to let
                
                // Check if we need to update the UI due to new columns
                fetch('/get_all_sample_data')
                    .then(response => response.json())
                    .then(freshData => {
                        if (freshData.success) {
                            // Check for any new columns added by the chatbot
                            const currentColumns = document.querySelectorAll('.table-striped tbody tr td:first-child');
                            const existingColumnNames = Array.from(currentColumns).map(col => col.textContent.trim());
                            const newColumns = Object.keys(freshData.sample_data).filter(col => !existingColumnNames.includes(col));
                            
                            if (newColumns.length > 0) {
                                console.log("New columns detected after modal close:", newColumns);
                                // This requires a page refresh to properly update the UI with new columns
                                alert("New columns have been added to your data. The page will refresh to show the updated table.");
                                window.location.reload();
                                return;
                            }
                        }
                        
                        // If no new columns, just regenerate the CSV preview
                        generateCsvPreview();
                    })
                    .catch(error => {
                        console.error("Error checking for new columns:", error);
                        // Fall back to just regenerating the CSV preview
                        generateCsvPreview();
                    });
            }
            
            // Wire up the close button properly
            if (closeButton) {
                // Remove any existing onclick attribute
                closeButton.removeAttribute('onclick');
                closeButton.addEventListener('click', closeModal);
            }
            
            // Also wire up the close button in the footer
            const modalCloseBtn = document.querySelector('#aiChatbotModal .modal-footer .btn-secondary');
            if (modalCloseBtn) {
                modalCloseBtn.addEventListener('click', closeModal);
            }
        } else {
            console.error("Chat UI elements not found in the DOM");
        }
    }
});
