// PDF Results JavaScript functionality
document.addEventListener('DOMContentLoaded', function() {
    console.log("PDF Results page loaded");
    
    // CSV preview elements
    const csvPreviewContainer = document.getElementById('csvPreviewContainer');
    const csvLoadingIndicator = document.getElementById('csvLoadingIndicator');
    const editWithAiButton = document.getElementById('editWithAiButton');
    
    // AI Chatbot elements
    const chatMessages = document.getElementById('chatMessages');
    const chatInput = document.getElementById('chatInput');
    const sendMessageButton = document.getElementById('sendMessageButton');
    const applyChangesButton = document.getElementById('applyChangesButton');
    
    // Initialize chatbot state
    const chatbotState = {
        messages: [],
        editedCsvData: null,
        pendingChanges: false,
        previousState: null
    };
    
    // Generate CSV preview on page load
    generateCsvPreview();
    
    // Setup AI Chatbot
    setupAIChatbot();
    
    // Check IPAFFS compatibility
    checkIpaffsCompatibility();
    
    function generateCsvPreview() {
        console.log("Generating CSV preview for PDF data");
        
        // Show loading indicator
        csvLoadingIndicator.classList.remove('d-none');
        csvPreviewContainer.innerHTML = '';
        
        // Get PDF data from the page (extract from the existing table)
        const pdfData = extractPdfDataFromTable();
        
        if (!pdfData || Object.keys(pdfData).length === 0) {
            csvPreviewContainer.innerHTML = '<div class="alert alert-warning">No data available for CSV preview.</div>';
            csvLoadingIndicator.classList.add('d-none');
            return;
        }
        
        // Create CSV preview table
        createCsvPreviewTable(pdfData);
        
        // Auto-save any default commodity selections if dropdowns are present
        setTimeout(() => {
            autoSaveDefaultCommoditySelections();
        }, 100);
        
        // Hide loading indicator
        csvLoadingIndicator.classList.add('d-none');
    }
    
    function extractPdfDataFromTable() {
        const data = {};
        const rows = document.querySelectorAll('.table-bordered tbody tr');
        
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 2) {
                const field = cells[0].textContent.trim();
                const valueCell = cells[1];
                
                // Check if the value cell contains array-of-objects cards
                const arrayContainer = valueCell.querySelector('.array-of-objects-container');
                if (arrayContainer) {
                    // Extract array values from card elements
                    const cardElements = arrayContainer.querySelectorAll('.card .card-body');
                    const arrayValues = Array.from(cardElements).map(cardBody => {
                        const obj = {};
                        const rows = cardBody.querySelectorAll('.row .col-md-6');
                        
                        rows.forEach(col => {
                            const labelElement = col.querySelector('small');
                            const valueElement = col.querySelector('.fw-medium');
                            
                            if (labelElement && valueElement) {
                                const key = labelElement.textContent.replace(':', '').trim();
                                let value = valueElement.textContent.trim();
                                
                                // Handle empty values
                                if (value === 'empty') {
                                    value = '';
                                } else {
                                    // Try to convert numbers
                                    const numValue = Number(value);
                                    if (!isNaN(numValue) && value !== '') {
                                        value = numValue;
                                    }
                                }
                                
                                obj[key] = value;
                            }
                        });
                        
                        return obj;
                    }).filter(item => Object.keys(item).length > 0);
                    
                    data[field] = arrayValues;
                } else {
                    // Check if the value cell contains a regular list (ul element)
                    const listElement = valueCell.querySelector('ul');
                    if (listElement) {
                        // Extract array values from list items
                        const listItems = listElement.querySelectorAll('li');
                        const arrayValues = Array.from(listItems).map(li => {
                            const text = li.textContent.trim();
                            
                            // Try to parse as JSON object first
                            try {
                                return JSON.parse(text);
                            } catch (e) {
                                // If JSON parsing fails, try to handle Python dict format
                                try {
                                    // Convert Python dict format to JSON format
                                    // Replace single quotes with double quotes, but be careful with quotes inside strings
                                    const jsonText = text
                                        .replace(/'/g, '"')  // Convert single quotes to double quotes
                                        .replace(/None/g, 'null')  // Convert Python None to JSON null
                                        .replace(/True/g, 'true')  // Convert Python True to JSON true
                                        .replace(/False/g, 'false'); // Convert Python False to JSON false
                                    
                                    return JSON.parse(jsonText);
                                } catch (e2) {
                                    // If both parsing attempts fail, return as string
                                    return text;
                                }
                            }
                        }).filter(item => item !== '');
                        
                        data[field] = arrayValues;
                    } else {
                        // Check if it's a JSON object (pre element)
                        const preElement = valueCell.querySelector('pre');
                        if (preElement) {
                            try {
                                const jsonValue = JSON.parse(preElement.textContent.trim());
                                data[field] = jsonValue;
                            } catch (e) {
                                // If JSON parsing fails, use as string
                                data[field] = preElement.textContent.trim();
                            }
                        } else {
                            // Regular text value
                            const value = valueCell.textContent.trim();
                            if (field && value && value !== 'No value') {
                                data[field] = value;
                            } else if (field) {
                                data[field] = '';
                            }
                        }
                    }
                }
            }
        });
        
        console.log("Extracted PDF data:", data);
        return data;
    }
    
    function createCsvPreviewTable(data) {
        console.log('Creating CSV preview table with data:', data);
        
        const previewDiv = document.createElement('div');
        previewDiv.style.overflowX = 'auto';
        previewDiv.style.maxHeight = '500px';
        previewDiv.style.overflowY = 'auto';
        
        // Check if this is array-of-objects format
        // This specifically looks for arrays containing objects (not strings or primitives)
        const arrayOfObjectsFields = Object.keys(data).filter(field => {
            const value = data[field];
            return Array.isArray(value) && 
                   value.length > 0 && 
                   typeof value[0] === 'object' && 
                   value[0] !== null &&
                   !Array.isArray(value[0]); // Ensure it's an object, not a nested array
        });
        
        console.log('Array-of-objects fields found:', arrayOfObjectsFields);
        console.log('All data fields:', Object.keys(data));
        console.log('Regular arrays (if any):', Object.keys(data).filter(field => 
            Array.isArray(data[field]) && 
            data[field].length > 0 && 
            typeof data[field][0] !== 'object'
        ));
        
        let tableRows = [];
        let fields = [];
        
        if (arrayOfObjectsFields.length > 0) {
            // This is array-of-objects format
            const arrayField = arrayOfObjectsFields[0]; // Use the first array field
            const objectsArray = data[arrayField];
            
            console.log('Processing array-of-objects format for field:', arrayField);
            console.log('Objects array:', objectsArray);
            
            // Extract all unique keys from all objects as column headers
            const allKeys = new Set();
            objectsArray.forEach(obj => {
                if (typeof obj === 'object' && obj !== null) {
                    Object.keys(obj).forEach(key => allKeys.add(key));
                }
            });
            
            fields = Array.from(allKeys);
            console.log('Column headers (object keys):', fields);
            
            // Create rows - each object becomes a row
            objectsArray.forEach(obj => {
                if (typeof obj === 'object' && obj !== null) {
                    const rowData = fields.map(field => {
                        const value = obj[field];
                        if (value === undefined || value === null) {
                            return ''; // Empty string for missing values
                        }
                        if (typeof value === 'object') {
                            return JSON.stringify(value);
                        }
                        return String(value);
                    });
                    tableRows.push(rowData);
                }
            });
            
            console.log('Created table rows:', tableRows);
        } else {
            // Regular format (not array-of-objects)
            console.log('Processing regular format (not array-of-objects)');
            
            // Get target columns from the schema if available
            const targetColumns = getTargetColumnsFromSchema();
            fields = targetColumns.length > 0 ? targetColumns : Object.keys(data);
            
            // Check if any field contains an array
            const arrayFields = fields.filter(field => Array.isArray(data[field]));
            const hasArrays = arrayFields.length > 0;
            
            let maxRows = 1;
            
            if (hasArrays) {
                // Find the maximum array length to determine number of rows
                arrayFields.forEach(field => {
                    if (Array.isArray(data[field])) {
                        maxRows = Math.max(maxRows, data[field].length);
                    }
                });
                
                // Create rows for array data
                for (let rowIndex = 0; rowIndex < maxRows; rowIndex++) {
                    const rowData = [];
                    fields.forEach(field => {
                        const value = data[field];
                        if (Array.isArray(value)) {
                            // Use array item if available, otherwise empty string
                            rowData.push(rowIndex < value.length ? value[rowIndex] : '');
                        } else {
                            // For non-array fields, use the value only in the first row
                            rowData.push(rowIndex === 0 ? (value || '') : '');
                        }
                    });
                    tableRows.push(rowData);
                }
            } else {
                // No arrays, create single row
                const rowData = fields.map(field => {
                    const value = data[field];
                    if (typeof value === 'object' && value !== null) {
                        return JSON.stringify(value);
                    }
                    return value || '';
                });
                tableRows.push(rowData);
            }
        }
        
        // Create HTML table
        const headerRowHTML = `
            <tr>
                <th style="background-color: #f8f9fa; font-weight: bold; text-align: center; width: 40px; min-width: 40px; max-width: 40px; position: sticky; left: 0; z-index: 1; border: 1px solid #dee2e6; padding: 8px;">#</th>
                ${fields.map(field => `
                    <th style="background-color: #b8daff; color: #000; font-weight: bold; text-align: center; padding: 10px 15px; border: 1px solid #dee2e6; min-width: 150px; max-width: 250px; white-space: normal; word-wrap: break-word;">${field}</th>
                `).join('')}
            </tr>
        `;
        
        const bodyRowsHTML = tableRows.map((row, index) => `
            <tr>
                <td style="background-color: #f8f9fa; font-weight: bold; text-align: center; width: 40px; min-width: 40px; max-width: 40px; position: sticky; left: 0; z-index: 1; border: 1px solid #dee2e6; padding: 8px;">${index + 1}</td>
                ${row.map(cellValue => `
                    <td style="padding: 6px 10px; border: 1px solid #dee2e6; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 150px;" title="${String(cellValue).replace(/"/g, '&quot;')}">${cellValue}</td>
                `).join('')}
            </tr>
        `).join('');
        
        const tableHTML = `
            <table style="border-collapse: collapse; width: auto; margin-bottom: 0;">
                <thead>
                    ${headerRowHTML}
                </thead>
                <tbody>
                    ${bodyRowsHTML}
                </tbody>
            </table>
        `;
        
        previewDiv.innerHTML = tableHTML;
        csvPreviewContainer.appendChild(previewDiv);
        
        // Add status message
        const statusText = document.createElement('div');
        statusText.className = 'text-muted small mt-2';
        const rowCount = tableRows.length;
        const isArrayOfObjects = arrayOfObjectsFields.length > 0;
        const arrayInfo = isArrayOfObjects ? ` (Array of Objects format - each object becomes a row)` : '';
        statusText.textContent = `CSV preview showing ${fields.length} field${fields.length > 1 ? 's' : ''} and ${rowCount} row${rowCount > 1 ? 's' : ''} extracted from the PDF document${arrayInfo}.`;
        
        csvPreviewContainer.appendChild(statusText);
    }
    
    function getTargetColumnsFromSchema() {
        // Try to get schema from the page's script tag or global variable
        try {
            // Look for schema data in the page
            const schemaScript = document.querySelector('script[data-schema]');
            if (schemaScript) {
                const schema = JSON.parse(schemaScript.getAttribute('data-schema'));
                if (schema && schema.properties) {
                    return Object.keys(schema.properties);
                }
            }
            
            // Try to get from window object if available
            if (window.schemaData && window.schemaData.properties) {
                return Object.keys(window.schemaData.properties);
            }
            
            // Return empty array if no schema found
            return [];
        } catch (e) {
            console.log('Could not extract target columns from schema:', e);
            return [];
        }
    }
    
    function setupAIChatbot() {
        if (!editWithAiButton || !chatMessages || !chatInput || !sendMessageButton || !applyChangesButton) {
            console.error("AI chatbot elements not found");
            return;
        }
        
        // Edit with AI button to open modal and capture data
        editWithAiButton.addEventListener('click', function() {
            // Show the modal
            const modal = new bootstrap.Modal(document.getElementById('aiChatbotModal'));
            modal.show();
            
            // Capture CSV data
            captureCsvData();
        });
        
        // Send message event listeners
        sendMessageButton.addEventListener('click', sendUserMessage);
        chatInput.addEventListener('keypress', function(event) {
            if (event.key === 'Enter') {
                event.preventDefault();
                sendUserMessage();
            }
        });
        
        // Apply changes button
        applyChangesButton.addEventListener('click', applyChanges);
        
        // Modal close event
        const modal = document.getElementById('aiChatbotModal');
        modal.addEventListener('hidden.bs.modal', function() {
            generateCsvPreview();
        });
    }
    
    function captureCsvData() {
        // Reset chatbot state
        chatbotState.messages = [];
        chatbotState.editedCsvData = null;
        chatbotState.pendingChanges = false;
        chatbotState.previousState = null;
        
        // Reset UI elements
        chatInput.placeholder = "Type your message here...";
        chatInput.classList.remove("clarification-needed");
        sendMessageButton.innerHTML = '<i class="bi bi-send"></i> Send';
        
        // Clear chat messages except the welcome message
        while (chatMessages.children.length > 1) {
            chatMessages.removeChild(chatMessages.lastChild);
        }
        
        // Get PDF data and convert to CSV format
        const pdfData = extractPdfDataFromTable();
        console.log('PDF data for chatbot:', pdfData);
        
        // Check if this is array-of-objects format
        const arrayOfObjectsFields = Object.keys(pdfData).filter(field => 
            Array.isArray(pdfData[field]) && 
            pdfData[field].length > 0 && 
            typeof pdfData[field][0] === 'object' && 
            pdfData[field][0] !== null
        );
        
        let csvData;
        
        if (arrayOfObjectsFields.length > 0) {
            // This is array-of-objects format
            const arrayField = arrayOfObjectsFields[0]; // Use the first array field
            const objectsArray = pdfData[arrayField];
            
            console.log('Processing array-of-objects format for chatbot:', arrayField, objectsArray);
            
            // Extract all unique keys from all objects as column headers
            const allKeys = new Set();
            objectsArray.forEach(obj => {
                if (typeof obj === 'object' && obj !== null) {
                    Object.keys(obj).forEach(key => allKeys.add(key));
                }
            });
            
            const headers = Array.from(allKeys);
            
            // Create rows - each object becomes a row
            const csvRows = objectsArray.map(obj => {
                const row = {};
                headers.forEach(field => {
                    const value = obj[field];
                    if (value === undefined || value === null) {
                        row[field] = ''; // Empty string for missing values
                    } else if (typeof value === 'object') {
                        row[field] = JSON.stringify(value);
                    } else {
                        row[field] = String(value);
                    }
                });
                return row;
            });
            
            csvData = {
                headers: headers,
                data: csvRows
            };
        } else {
            // Regular format (not array-of-objects)
            const headers = Object.keys(pdfData);
            
            // Check if any field contains an array
            const arrayFields = headers.filter(field => Array.isArray(pdfData[field]));
            const hasArrays = arrayFields.length > 0;
            
            let csvRows = [];
            
            if (hasArrays) {
                // Find the maximum array length to determine number of rows
                let maxRows = 1;
                arrayFields.forEach(field => {
                    if (Array.isArray(pdfData[field])) {
                        maxRows = Math.max(maxRows, pdfData[field].length);
                    }
                });
                
                // Create rows for array data
                for (let rowIndex = 0; rowIndex < maxRows; rowIndex++) {
                    const row = {};
                    headers.forEach(field => {
                        const value = pdfData[field];
                        if (Array.isArray(value)) {
                            // Use array item if available, otherwise empty string
                            row[field] = rowIndex < value.length ? value[rowIndex] : '';
                        } else {
                            // For non-array fields, use the value only in the first row
                            row[field] = rowIndex === 0 ? (value || '') : '';
                        }
                    });
                    csvRows.push(row);
                }
            } else {
                // No arrays, create single row
                const row = {};
                headers.forEach(field => {
                    const value = pdfData[field];
                    if (typeof value === 'object' && value !== null) {
                        row[field] = JSON.stringify(value);
                    } else {
                        row[field] = value || '';
                    }
                });
                csvRows.push(row);
            }
            
            csvData = {
                headers: headers,
                data: csvRows
            };
        }
        
        // Show thinking indicator
        showThinkingIndicator();
        
        // Store the CSV data
        chatbotState.editedCsvData = csvData;
        
        // Hide thinking indicator
        hideThinkingIndicator();
        
        // Add initial AI message
        const headerInfo = `The CSV has ${csvData.headers.length} fields: ${csvData.headers.join(', ')}.`;
        const rowCount = csvData.data.length;
        const rowInfo = `There ${rowCount === 1 ? 'is' : 'are'} ${rowCount} row${rowCount > 1 ? 's' : ''} of data extracted from the PDF.`;
        const formatInfo = arrayOfObjectsFields.length > 0 ? ` (Array of Objects format - each object becomes a row)` : '';
        
        addMessage('ai', `I've loaded the CSV data. ${headerInfo} ${rowInfo}${formatInfo} What would you like to edit?`);
    }
    
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
        
        // Prepare request data
        const requestBody = {
            message: message,
            csv_data: chatbotState.editedCsvData,
            source_data: {} // PDF doesn't have source Excel data
        };
        
        // If we have a stored state from a previous clarification request, include it
        if (chatbotState.previousState && chatbotState.previousState.awaiting_clarification) {
            requestBody.prev_state = chatbotState.previousState;
        }
        
        // If we have thread_id, add it to the request
        if (chatbotState.threadId) {
            requestBody.thread_id = chatbotState.threadId;
            requestBody.csv_file_path = chatbotState.csvFilePath;
            requestBody.original_request = chatbotState.originalRequest;
            requestBody.rewritten_request = chatbotState.rewrittenRequest;
            requestBody.in_clarification_mode = chatbotState.inClarificationMode;
            requestBody.is_request_clarified = chatbotState.isRequestClarified;
            requestBody.clarification_count = chatbotState.clarificationCount;
            requestBody.last_active_node = chatbotState.lastActiveNode;
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
                // Check if we need human input (human-in-the-loop capability)
                if (data.needs_input && data.interrupt_message) {
                    console.log("Server requested human input:", data.interrupt_message);
                    
                    // Store conversation state
                    chatbotState.threadId = data.thread_id;
                    chatbotState.csvFilePath = data.csv_file_path;
                    chatbotState.originalRequest = data.original_request;
                    chatbotState.rewrittenRequest = data.rewritten_request;
                    chatbotState.inClarificationMode = data.in_clarification_mode;
                    chatbotState.isRequestClarified = data.is_request_clarified;
                    chatbotState.clarificationCount = data.clarification_count;
                    chatbotState.lastActiveNode = data.last_active_node;
                    
                    // Add the interrupt message to the chat
                    addMessage('ai', data.interrupt_message);
                    
                    // Update the UI to indicate waiting for human input
                    chatInput.placeholder = "Please provide your input...";
                    chatInput.classList.add("human-input-needed");
                    sendMessageButton.innerHTML = '<i class="bi bi-send"></i> Send Input';
                    
                    // Add visual cue to the last AI message
                    const lastMessage = chatMessages.querySelector('.ai-message:last-of-type .message-content');
                    if (lastMessage) {
                        lastMessage.classList.add("awaiting-input");
                    }
                } else {
                    // Normal response without interrupt
                    
                    // Add AI response to chat
                    addMessage('ai', data.response);
                    
                    // Clear previous human input state
                    chatbotState.threadId = null;
                    chatbotState.csvFilePath = null;
                    chatbotState.inClarificationMode = false;
                    chatbotState.isRequestClarified = false;
                    
                    // Reset UI elements
                    chatInput.placeholder = "Type your message here...";
                    chatInput.classList.remove("human-input-needed");
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
            hideThinkingIndicator();
            addMessage('ai', 'I encountered an error processing your request. Please try again.');
        });
    }
    
    function applyChanges() {
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
                
                // Update the CSV preview
                generateCsvPreview();
                
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
    }
    
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
    
    function hideThinkingIndicator() {
        const thinkingIndicator = document.getElementById('thinkingIndicator');
        if (thinkingIndicator) {
            thinkingIndicator.remove();
        }
    }
    
    function checkIpaffsCompatibility() {
        console.log("Checking IPAFFS compatibility");
        
        const ipaffsLoadingIndicator = document.getElementById('ipaffsLoadingIndicator');
        const ipaffsCompatibilityResult = document.getElementById('ipaffsCompatibilityResult');
        const ipaffsCompatibleContent = document.getElementById('ipaffsCompatibleContent');
        const ipaffsIncompatibleContent = document.getElementById('ipaffsIncompatibleContent');
        
        if (!ipaffsLoadingIndicator || !ipaffsCompatibilityResult) {
            console.error("IPAFFS elements not found");
            return;
        }
        
        // Show loading indicator
        ipaffsLoadingIndicator.style.display = 'block';
        ipaffsCompatibilityResult.style.display = 'none';
        
        // Send request to check compatibility
        fetch('/check_ipaffs_compatibility', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({})
        })
        .then(response => response.json())
        .then(data => {
            // Hide loading indicator
            ipaffsLoadingIndicator.style.display = 'none';
            ipaffsCompatibilityResult.style.display = 'block';
            
            if (data.compatible) {
                // Show compatible content
                ipaffsCompatibleContent.style.display = 'block';
                ipaffsIncompatibleContent.style.display = 'none';
                
                // Update match counts
                const matchedCountEl = document.getElementById('ipaffsMatchedCount');
                const totalCountEl = document.getElementById('ipaffsTotalCount');
                if (matchedCountEl) matchedCountEl.textContent = data.total_matched;
                if (totalCountEl) totalCountEl.textContent = data.total_required;
                
                // Display matched headers
                const matchedHeadersList = document.getElementById('matchedHeadersList');
                if (matchedHeadersList && data.matched_headers) {
                    matchedHeadersList.innerHTML = '';
                    Object.keys(data.matched_headers).forEach(ipaffsHeader => {
                        const badge = document.createElement('span');
                        badge.className = 'badge bg-success';
                        badge.textContent = `${ipaffsHeader} → ${data.matched_headers[ipaffsHeader]}`;
                        matchedHeadersList.appendChild(badge);
                    });
                }
                
                // Setup pre-fill button
                const prefillButton = document.getElementById('prefillIpaffsButton');
                if (prefillButton) {
                    prefillButton.addEventListener('click', prefillIpaffs);
                }
                
            } else {
                // Show incompatible content
                ipaffsCompatibleContent.style.display = 'none';
                ipaffsIncompatibleContent.style.display = 'block';
                
                // Display missing headers
                const missingHeadersList = document.getElementById('missingHeadersList');
                if (missingHeadersList && data.missing_headers) {
                    missingHeadersList.innerHTML = '';
                    data.missing_headers.forEach(header => {
                        const badge = document.createElement('span');
                        badge.className = 'badge bg-warning text-dark';
                        badge.textContent = header;
                        missingHeadersList.appendChild(badge);
                    });
                }
            }
        })
        .catch(error => {
            console.error('Error checking IPAFFS compatibility:', error);
            
            // Hide loading indicator and show error
            ipaffsLoadingIndicator.style.display = 'none';
            ipaffsCompatibilityResult.style.display = 'block';
            ipaffsIncompatibleContent.style.display = 'block';
            ipaffsCompatibleContent.style.display = 'none';
            
            const missingHeadersList = document.getElementById('missingHeadersList');
            if (missingHeadersList) {
                missingHeadersList.innerHTML = '<span class="badge bg-danger">Error checking compatibility</span>';
            }
        });
    }
    
    function prefillIpaffs() {
        console.log("Pre-filling IPAFFS data");
        
        const prefillButton = document.getElementById('prefillIpaffsButton');
        if (prefillButton) {
            // Show loading state
            prefillButton.disabled = true;
            prefillButton.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Pre-filling...';
        }
        
        // Send request to pre-fill IPAFFS data
        fetch('/prefill_ipaffs', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({})
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                console.log("IPAFFS pre-fill successful:", data);
                
                // Show success message
                const alertContainer = document.getElementById('ipaffsCompatibleContent');
                if (alertContainer) {
                    // Create success alert
                    const successAlert = document.createElement('div');
                    successAlert.className = 'alert alert-info mt-3';
                    successAlert.innerHTML = `
                        <i class="bi bi-check-circle"></i> 
                        <strong>IPAFFS Pre-fill Complete!</strong><br>
                        ${data.message}
                        ${data.commodity_options && data.commodity_options.length > 0 ? 
                            `<br><small>Commodity code dropdowns have been prepared for user selection.</small>` : ''}
                    `;
                    
                    // Remove any existing success alerts
                    const existingAlerts = alertContainer.querySelectorAll('.alert-info');
                    existingAlerts.forEach(alert => alert.remove());
                    
                    // Add new success alert
                    alertContainer.appendChild(successAlert);
                    
                    // Auto-remove after 10 seconds
                    setTimeout(() => {
                        if (successAlert.parentNode) {
                            successAlert.remove();
                        }
                    }, 10000);
                }
                
                // Store commodity options for dropdown creation
                window.commodityOptions = data.commodity_options || [];
                
                // Regenerate CSV preview with updated data from session
                setTimeout(() => {
                    generateCsvPreviewFromSession();
                }, 500); // Small delay to ensure session is updated
                
                // Update pre-fill button to show completion
                if (prefillButton) {
                    prefillButton.disabled = false;
                    prefillButton.innerHTML = '<i class="bi bi-check-circle"></i> Pre-filled';
                    prefillButton.classList.remove('btn-success');
                    prefillButton.classList.add('btn-outline-success');
                    
                    // Reset button after some time
                    setTimeout(() => {
                        prefillButton.innerHTML = '<i class="bi bi-database-fill"></i> Pre-fill IPAFFS';
                        prefillButton.classList.remove('btn-outline-success');
                        prefillButton.classList.add('btn-success');
                    }, 5000);
                }
                
            } else {
                console.error("IPAFFS pre-fill failed:", data.error);
                
                // Show error message
                const alertContainer = document.getElementById('ipaffsCompatibleContent');
                if (alertContainer) {
                    const errorAlert = document.createElement('div');
                    errorAlert.className = 'alert alert-danger mt-3';
                    errorAlert.innerHTML = `
                        <i class="bi bi-exclamation-triangle"></i> 
                        <strong>Pre-fill Failed:</strong> ${data.error}
                    `;
                    
                    // Remove any existing error alerts
                    const existingAlerts = alertContainer.querySelectorAll('.alert-danger');
                    existingAlerts.forEach(alert => alert.remove());
                    
                    alertContainer.appendChild(errorAlert);
                    
                    // Auto-remove after 8 seconds
                    setTimeout(() => {
                        if (errorAlert.parentNode) {
                            errorAlert.remove();
                        }
                    }, 8000);
                }
                
                // Reset button
                if (prefillButton) {
                    prefillButton.disabled = false;
                    prefillButton.innerHTML = '<i class="bi bi-database-fill"></i> Pre-fill IPAFFS';
                }
            }
        })
        .catch(error => {
            console.error('Error pre-filling IPAFFS data:', error);
            
            // Show error message
            const alertContainer = document.getElementById('ipaffsCompatibleContent');
            if (alertContainer) {
                const errorAlert = document.createElement('div');
                errorAlert.className = 'alert alert-danger mt-3';
                errorAlert.innerHTML = `
                    <i class="bi bi-exclamation-triangle"></i> 
                    <strong>Pre-fill Error:</strong> Unable to connect to EPPO database
                `;
                alertContainer.appendChild(errorAlert);
                
                // Auto-remove after 8 seconds
                setTimeout(() => {
                    if (errorAlert.parentNode) {
                        errorAlert.remove();
                    }
                }, 8000);
            }
            
            // Reset button
            if (prefillButton) {
                prefillButton.disabled = false;
                prefillButton.innerHTML = '<i class="bi bi-database-fill"></i> Pre-fill IPAFFS';
            }
        });
    }
    
    function generateCsvPreviewFromSession() {
        console.log("Generating CSV preview from updated session data");
        
        // Show loading indicator
        csvLoadingIndicator.classList.remove('d-none');
        csvPreviewContainer.innerHTML = '';
        
        // Fetch the current CSV data from the backend session
        fetch('/get_current_csv_data', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                console.log("Fetched updated CSV data from session:", data);
                
                // Create CSV preview table with updated data
                createCsvPreviewTableWithDropdowns(data.csv_data, data.format);
                
                // Auto-save any default commodity selections if dropdowns are present
                setTimeout(() => {
                    autoSaveDefaultCommoditySelections();
                }, 100);
                
            } else {
                console.error("Failed to fetch updated CSV data:", data.error);
                csvPreviewContainer.innerHTML = '<div class="alert alert-warning">Failed to load updated CSV data.</div>';
            }
            
            // Hide loading indicator
            csvLoadingIndicator.classList.add('d-none');
        })
        .catch(error => {
            console.error('Error fetching updated CSV data:', error);
            csvPreviewContainer.innerHTML = '<div class="alert alert-danger">Error loading updated CSV data.</div>';
            csvLoadingIndicator.classList.add('d-none');
        });
    }
    
    function createCsvPreviewTableWithDropdowns(csvData, format) {
        console.log('Creating CSV preview table with dropdowns:', csvData);
        
        const previewDiv = document.createElement('div');
        previewDiv.style.overflowX = 'auto';
        previewDiv.style.maxHeight = '500px';
        previewDiv.style.overflowY = 'auto';
        
        const headers = csvData.headers || [];
        const tableRows = csvData.data || [];
        
        if (headers.length === 0 || tableRows.length === 0) {
            csvPreviewContainer.innerHTML = '<div class="alert alert-warning">No data available for CSV preview.</div>';
            return;
        }
        
        // Find commodity code column index
        const commodityCodeColIndex = headers.findIndex(header => 
            header.toLowerCase().includes('commodity') && header.toLowerCase().includes('code')
        );
        
        // Find EPPO code column index to highlight pre-filled data (flexible matching)
        const eppoCodeColIndex = headers.findIndex(header => {
            const headerLower = header.toLowerCase().trim();
            return (headerLower === 'eppocode' || 
                    headerLower === 'eppo code' || 
                    headerLower === 'eppo_code' || 
                    headerLower === 'eppo-code' ||
                    (headerLower.includes('eppo') && headerLower.includes('code')));
        });
        
        // Find intended users column index to highlight pre-filled data (flexible matching)
        const intendedUsersColIndex = headers.findIndex(header => {
            const headerLower = header.toLowerCase().trim();
            return (headerLower === 'intended for final users' ||
                    headerLower === 'intended for final users (or commercial flower production)' ||
                    headerLower === 'intended final users' ||
                    headerLower === 'final users' ||
                    headerLower === 'intended users' ||
                    (headerLower.includes('intended') && (headerLower.includes('final') || headerLower.includes('users'))) ||
                    headerLower.includes('commercial flower production'));
        });
        
        console.log('Column indices - Commodity:', commodityCodeColIndex, 'EPPO:', eppoCodeColIndex, 'Intended:', intendedUsersColIndex);
        
        // Create HTML table header
        const headerRowHTML = `
            <tr>
                <th style="background-color: #f8f9fa; font-weight: bold; text-align: center; width: 40px; min-width: 40px; max-width: 40px; position: sticky; left: 0; z-index: 1; border: 1px solid #dee2e6; padding: 8px;">#</th>
                ${headers.map(field => `
                    <th style="background-color: #b8daff; color: #000; font-weight: bold; text-align: center; padding: 10px 15px; border: 1px solid #dee2e6; min-width: 150px; max-width: 250px; white-space: normal; word-wrap: break-word;">${field}</th>
                `).join('')}
            </tr>
        `;
        
        // Create HTML table body with dropdowns for commodity codes and highlighting for pre-filled data
        const bodyRowsHTML = tableRows.map((row, rowIndex) => {
            const rowCells = headers.map((header, colIndex) => {
                const cellValue = row[header] || '';
                
                // Check if this is a commodity code column and we have options
                if (colIndex === commodityCodeColIndex && window.commodityOptions && window.commodityOptions[rowIndex]) {
                    const options = window.commodityOptions[rowIndex];
                    
                    // If we have an existing extracted commodity code, preserve it instead of showing dropdown
                    if (cellValue && cellValue.trim() && options && options.length > 0) {
                        // Show existing commodity code as preserved/read-only
                        return `
                            <td style="padding: 6px 10px; border: 1px solid #dee2e6; background-color: #fff3cd; max-width: 200px;" 
                                title="Extracted commodity code (preserved): ${String(cellValue).replace(/"/g, '&quot;')}">
                                <i class="bi bi-shield-check text-warning me-1"></i>
                                <strong>${cellValue}</strong>
                                <small class="text-muted d-block">Extracted from document</small>
                            </td>
                        `;
                    } else if (options && options.length > 0) {
                        // Create dropdown for commodity code selection (only when no existing code)
                        const selectId = `commodity_select_${rowIndex}`;
                        // For rows without existing codes, use default selection
                        let defaultSelected = false;
                        const optionsHTML = options.map((option, index) => {
                            let isSelected = false;
                            
                            // Only auto-select first option if no existing cellValue
                            if (index === 0 && !defaultSelected && !cellValue) {
                                isSelected = true;
                                defaultSelected = true;
                            }
                            
                            return `<option value="${option.code}" ${isSelected ? 'selected' : ''}>${option.display}</option>`;
                        }).join('');
                        
                        return `
                            <td style="padding: 6px 10px; border: 1px solid #dee2e6; max-width: 200px;">
                                <select id="${selectId}" class="form-select form-select-sm commodity-dropdown" 
                                        data-row="${rowIndex}" style="font-size: 12px; min-width: 180px;">
                                    ${options.length > 0 ? '' : '<option value="">No valid commodity codes available</option>'}
                                    ${optionsHTML}
                                </select>
                                <small class="text-muted">Select from database</small>
                            </td>
                        `;
                    }
                }
                
                // Highlight pre-filled EPPO codes
                if (colIndex === eppoCodeColIndex && cellValue) {
                    return `
                        <td style="padding: 6px 10px; border: 1px solid #dee2e6; background-color: #d1ecf1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 150px;" 
                            title="Pre-filled EPPO code: ${String(cellValue).replace(/"/g, '&quot;')}">
                            <i class="bi bi-check-circle text-info me-1"></i>${cellValue}
                        </td>
                    `;
                }
                
                // Highlight pre-filled intended users data
                if (colIndex === intendedUsersColIndex && cellValue === 'No') {
                    return `
                        <td style="padding: 6px 10px; border: 1px solid #dee2e6; background-color: #d4edda; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 150px;" 
                            title="Pre-filled intended users: ${String(cellValue).replace(/"/g, '&quot;')}">
                            <i class="bi bi-check-circle text-success me-1"></i>${cellValue}
                        </td>
                    `;
                }
                
                // Regular cell display
                return `
                    <td style="padding: 6px 10px; border: 1px solid #dee2e6; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 150px;" 
                        title="${String(cellValue).replace(/"/g, '&quot;')}">${cellValue}</td>
                `;
            }).join('');
            
            return `
                <tr>
                    <td style="background-color: #f8f9fa; font-weight: bold; text-align: center; width: 40px; min-width: 40px; max-width: 40px; position: sticky; left: 0; z-index: 1; border: 1px solid #dee2e6; padding: 8px;">${rowIndex + 1}</td>
                    ${rowCells}
                </tr>
            `;
        }).join('');
        
        const tableHTML = `
            <table style="border-collapse: collapse; width: auto; margin-bottom: 0;">
                <thead>
                    ${headerRowHTML}
                </thead>
                <tbody>
                    ${bodyRowsHTML}
                </tbody>
            </table>
        `;
        
        previewDiv.innerHTML = tableHTML;
        csvPreviewContainer.appendChild(previewDiv);
        
        // Setup dropdown change handlers
        setupCommodityDropdownHandlers();
        
        // Auto-save default selected values for commodity dropdowns
        autoSaveDefaultCommoditySelections();
        
        // Add status message
        const statusText = document.createElement('div');
        statusText.className = 'text-muted small mt-2';
        const rowCount = tableRows.length;
        const hasDropdowns = commodityCodeColIndex !== -1 && window.commodityOptions && window.commodityOptions.length > 0;
        const dropdownInfo = hasDropdowns ? ' Interactive commodity code dropdowns are available for selection.' : '';
        statusText.textContent = `CSV preview showing ${headers.length} field${headers.length > 1 ? 's' : ''} and ${rowCount} row${rowCount > 1 ? 's' : ''} with IPAFFS pre-filled data.${dropdownInfo}`;
        
        csvPreviewContainer.appendChild(statusText);
    }
    
    function setupCommodityDropdownHandlers() {
        const dropdowns = document.querySelectorAll('.commodity-dropdown');
        dropdowns.forEach(dropdown => {
            dropdown.addEventListener('change', function() {
                const rowIndex = parseInt(this.dataset.row);
                const selectedValue = this.value;
                const selectedText = this.options[this.selectedIndex].text;
                
                console.log(`Commodity code selected for row ${rowIndex}: ${selectedValue} (${selectedText})`);
                
                // Persist the selection to the backend
                saveCommoditySelection(rowIndex, selectedValue, selectedText, this);
            });
        });
    }
    
    function autoSaveDefaultCommoditySelections() {
        console.log("Auto-saving default commodity selections...");
        const dropdowns = document.querySelectorAll('.commodity-dropdown');
        
        // Create array of selections to save
        const selectionsToSave = [];
        dropdowns.forEach(dropdown => {
            // Get the currently selected option (which is the default)
            const rowIndex = parseInt(dropdown.dataset.row);
            const selectedValue = dropdown.value;
            const selectedText = dropdown.options[dropdown.selectedIndex].text;
            
            // Only save if we have a valid selection
            if (selectedValue) {
                selectionsToSave.push({
                    rowIndex: rowIndex,
                    selectedValue: selectedValue,
                    selectedText: selectedText
                });
                console.log(`Will auto-save row ${rowIndex} (no extracted code): ${selectedValue}`);
            }
        });
        
        // Note: We only save selections for rows that actually have dropdowns
        // Rows with extracted commodity codes are preserved and don't get dropdowns
        if (selectionsToSave.length > 0) {
            console.log(`Auto-saving ${selectionsToSave.length} default selections (preserved rows skipped)`);
            saveBatchCommoditySelections(selectionsToSave);
        } else {
            console.log("No dropdown selections to auto-save (all commodity codes preserved from extraction)");
            window.commodityAutoSaveComplete = true;
        }
    }
    
    function saveBatchCommoditySelections(selections) {
        if (selections.length === 0) {
            console.log("No commodity selections to save");
            return Promise.resolve();
        }
        
        console.log(`Batch saving ${selections.length} commodity selections...`);
        
        // Create a single request with all selections
        return fetch('/batch_update_commodity_selections', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                selections: selections
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                console.log(`Successfully batch saved ${selections.length} commodity selections`);
                // Mark that auto-save is complete
                window.commodityAutoSaveComplete = true;
            } else {
                console.error(`Failed to batch save commodity selections:`, data.error);
                // Fall back to individual saves if batch fails
                return saveSelectionsSequentially(selections, 0);
            }
        })
        .catch(error => {
            console.error(`Error batch saving commodity selections:`, error);
            // Fall back to individual saves if batch fails
            return saveSelectionsSequentially(selections, 0);
        });
    }
    
    function saveSelectionsSequentially(selections, index) {
        if (index >= selections.length) {
            console.log("Completed auto-saving all default commodity selections");
            window.commodityAutoSaveComplete = true;
            return Promise.resolve();
        }
        
        const selection = selections[index];
        console.log(`Auto-saving default commodity code for row ${selection.rowIndex}: ${selection.selectedValue} (${selection.selectedText})`);
        
        // Save this selection
        return fetch('/update_commodity_selection', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                row_index: selection.rowIndex,
                commodity_code: selection.selectedValue,
                display_text: selection.selectedText
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                console.log(`Auto-saved default commodity selection for row ${selection.rowIndex} successfully`);
            } else {
                console.error(`Failed to auto-save default commodity selection for row ${selection.rowIndex}:`, data.error);
            }
            
            // Continue with the next selection immediately (no delay needed)
            return saveSelectionsSequentially(selections, index + 1);
        })
        .catch(error => {
            console.error(`Error auto-saving default commodity selection for row ${selection.rowIndex}:`, error);
            
            // Continue with the next selection even if this one failed
            return saveSelectionsSequentially(selections, index + 1);
        });
    }
    
    function saveCommoditySelection(rowIndex, selectedValue, selectedText, dropdownElement, isDefault = false) {
        // Persist the selection to the backend
        fetch('/update_commodity_selection', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                row_index: rowIndex,
                commodity_code: selectedValue,
                display_text: selectedText
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                console.log(`Commodity selection saved successfully${isDefault ? ' (default)' : ''}`);
                
                // Show user feedback only for manual selections (not defaults)
                if (dropdownElement && !isDefault) {
                    dropdownElement.style.borderColor = '#28a745';
                    dropdownElement.style.backgroundColor = '#f8fff9';
                    
                    // Reset styling after a moment
                    setTimeout(() => {
                        dropdownElement.style.borderColor = '';
                        dropdownElement.style.backgroundColor = '';
                    }, 2000);
                }
            } else {
                console.error(`Failed to save commodity selection${isDefault ? ' (default)' : ''}:`, data.error);
                // Show error feedback only for manual selections (not defaults)
                if (dropdownElement && !isDefault) {
                    dropdownElement.style.borderColor = '#dc3545';
                    dropdownElement.style.backgroundColor = '#f8d7da';
                    
                    // Reset styling after a moment
                    setTimeout(() => {
                        dropdownElement.style.borderColor = '';
                        dropdownElement.style.backgroundColor = '';
                    }, 2000);
                }
            }
        })
        .catch(error => {
            console.error(`Error saving commodity selection${isDefault ? ' (default)' : ''}:`, error);
            // Show error feedback only for manual selections (not defaults)
            if (dropdownElement && !isDefault) {
                dropdownElement.style.borderColor = '#dc3545';
                dropdownElement.style.backgroundColor = '#f8d7da';
                
                // Reset styling after a moment
                setTimeout(() => {
                    dropdownElement.style.borderColor = '';
                    dropdownElement.style.backgroundColor = '';
                }, 2000);
            }
        });
    }
    
});
