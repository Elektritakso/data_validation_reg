/**
 * CSV Validator - Client-side functionality
 * Handles file upload, column mapping, and validation
 */

// DOM Elements
const elements = {
    fileInput: document.getElementById('file-input'),
    uploadForm: document.getElementById('upload-form'),
    loadingIndicator: document.getElementById('loading-indicator'),
    fileInfo: document.getElementById('file-info'),
    previewTable: document.getElementById('preview-table'),
    validateButton: document.getElementById('validate-button'),
    validationResults: document.getElementById('validation-results'),
    columnMappingCard: document.getElementById('column-mapping-card'),
    columnMappingsContainer: document.getElementById('column-mappings-container'),
    errorContainer: document.getElementById('error-container')
};

// Global state
let state = {
    fileData: null,
    availableSourceColumns: [],
    expectedTargetFields: [
        'code', 'firstname', 'lastname', 'email', 'birthdate', 
        'address', 'city', 'phone', 'cellphone', 'countrycode', 
        'signuplanguagecode', 'currencycode'
    ]
};

// -------------------------
// Initialization
// -------------------------

document.addEventListener('DOMContentLoaded', () => {
    // Initialize event listeners
    initEventListeners();
});

function initEventListeners() {
    // File upload form submission
    elements.uploadForm.addEventListener('submit', handleFileUpload);
    
    // Validate button click
    elements.validateButton.addEventListener('click', handleValidation);
    
    // Add mapping button click (if it exists)
    const addMappingButton = document.getElementById('add-mapping-button');
    if (addMappingButton) {
        addMappingButton.addEventListener('click', addEmptyMappingRow);
    }
}

// -------------------------
// File Upload Handling
// -------------------------

/**
 * Handle file upload form submission
 * @param {Event} event - Form submission event
 */
function handleFileUpload(event) {
    event.preventDefault();
    
    // Check if file is selected
    if (!elements.fileInput.files.length) {
        showError('Please select a file first');
        return;
    }
    
    const file = elements.fileInput.files[0];
    const formData = new FormData();
    formData.append('file', file);
    
    // Get all checked required columns
    const requiredColumns = [];
    document.querySelectorAll('.required-column:checked').forEach(function(checkbox) {
        requiredColumns.push(checkbox.value.trim());
    });
    
    // Add required columns as a JSON array string
    formData.append('required_columns', JSON.stringify(requiredColumns));
    
    // Show loading indicator
    elements.loadingIndicator.classList.remove('d-none');
    
    // Clear previous results
    elements.fileInfo.innerHTML = '';
    elements.previewTable.innerHTML = '';
    elements.validationResults.innerHTML = '';
    
    // Send file to server
    fetch('/upload', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) {
            return response.json().then(data => {
                throw new Error(JSON.stringify(data));
            });
        }
        return response.json();
    })
    .then(data => {
        // Hide loading indicator
        elements.loadingIndicator.classList.add('d-none');
        
        // Store file data for later validation
        state.fileData = data;
        
        // Display file info
        displayFileInfo(data, file.name, requiredColumns);
        
        // Display preview table
        displayPreviewTable(data);
        
        // Enable validate button
        elements.validateButton.disabled = false;
    })
    .catch(error => {
        elements.loadingIndicator.classList.add('d-none');
        console.error('Upload error:', error);
        
        try {
            // Try to parse the error data
            const errorData = JSON.parse(error.message);
            
            // If we have missing columns, show mapping UI
            if (errorData.error && errorData.missing_columns && errorData.available_columns) {
                showError(`Required columns missing: ${errorData.missing_columns.join(', ')}`);
                showMappingUI(errorData.available_columns, errorData.missing_columns);
            } else {
                showError(errorData.error || 'Error uploading file. Please try again.');
            }
        } catch (e) {
            // If parsing fails, show the original error
            showError(error.message || 'Error uploading file. Please try again.');
        }
    });
}

/**
 * Display file information after successful upload
 * @param {Object} data - Response data from server
 * @param {string} fileName - Original file name
 * @param {Array} requiredColumns - List of required columns
 */
function displayFileInfo(data, fileName, requiredColumns) {
    elements.fileInfo.innerHTML = `
        <div class="alert alert-info">
            <h5>File uploaded successfully</h5>
            <p>File: <strong>${fileName}</strong></p>
            <p>Total Rows: <strong>${data.rows}</strong></p>
            <p>Detected Delimiter: <strong>${data.detected_delimiter === '\t' ? 'Tab' : data.detected_delimiter}</strong></p>
            <p>Detected Encoding: <strong>${data.detected_encoding}</strong></p>
            <p>Required Columns:</p>
            <ul>
                ${requiredColumns.map(col => `<li>${col}</li>`).join('')}
            </ul>
        </div>
    `;
}

/**
 * Display preview table with sample data
 * @param {Object} data - Response data from server
 */
function displayPreviewTable(data) {
    let tableHTML = '<div class="table-responsive"><table class="table table-striped table-bordered">';
    
    // Add header row
    tableHTML += '<thead><tr>';
    data.columns.forEach(column => {
        tableHTML += `<th>${column}</th>`;
    });
    tableHTML += '</tr></thead>';
    
    // Add data rows
    tableHTML += '<tbody>';
    data.data.forEach(row => {
        tableHTML += '<tr>';
        data.columns.forEach(column => {
            tableHTML += `<td>${row[column] || ''}</td>`;
        });
        tableHTML += '</tr>';
    });
    tableHTML += '</tbody></table></div>';
    
    // Update preview table
    elements.previewTable.innerHTML = tableHTML;
}

// -------------------------
// Column Mapping Functionality
// -------------------------

/**
 * Show column mapping UI for missing columns
 * @param {Array} csvColumns - Available columns in the CSV
 * @param {Array} missingColumns - Required columns that are missing
 */
function showMappingUI(csvColumns, missingColumns = []) {
    console.log("Showing mapping UI for columns:", csvColumns);
    console.log("Missing columns:", missingColumns);
    
    // Update available source columns
    state.availableSourceColumns = csvColumns;
    
    // Clear existing mappings
    elements.columnMappingsContainer.innerHTML = '';
    
    // Get suggested mappings
    const suggestedMappings = suggestMappings(csvColumns, state.expectedTargetFields);
    console.log("Suggested mappings:", suggestedMappings);
    
    /*
    // Special case: If ID is present and code is missing, suggest ID -> code mapping
    if (csvColumns.includes('ID') && missingColumns.includes('code') && !suggestedMappings['ID']) {
        suggestedMappings['ID'] = 'code';
    }
    */
    // Add a row for each suggested mapping
    Object.entries(suggestedMappings).forEach(([source, target]) => {
        addMappingRow(source, target);
    });
    
    // Show the mapping card
    elements.columnMappingCard.style.display = 'block';
    
    // Add the Apply Mappings button
    addApplyMappingsButton();
    
    // Disable validate button until mappings are applied
    elements.validateButton.disabled = true;
}

/**
 * Suggest column mappings based on similarity
 * @param {Array} sourceColumns - Available columns in the CSV
 * @param {Array} targetFields - Required fields
 * @returns {Object} Suggested mappings
 */
function suggestMappings(sourceColumns, targetFields) {
    const mappings = {};
    const sourceNormalized = sourceColumns.map(col => normalizeColumnName(col));
    
    // First pass: Look for exact matches after normalization
    sourceColumns.forEach((sourceCol, index) => {
        const normalizedSource = sourceNormalized[index];
        
        targetFields.forEach(targetField => {
            const normalizedTarget = normalizeColumnName(targetField);
            
            if (normalizedSource === normalizedTarget) {
                mappings[sourceCol] = targetField;
            }
        });
    });
    
    // Second pass: Look for partial matches for unmapped columns
    sourceColumns.forEach((sourceCol) => {
        if (mappings[sourceCol]) return; // Skip already mapped columns
        
        const normalizedSource = normalizeColumnName(sourceCol);
        
        targetFields.forEach(targetField => {
            if (Object.values(mappings).includes(targetField)) return; // Skip already mapped targets
            
            const normalizedTarget = normalizeColumnName(targetField);
            
            // Check if source contains target or target contains source
            if (normalizedSource.includes(normalizedTarget) || normalizedTarget.includes(normalizedSource)) {
                mappings[sourceCol] = targetField;
            }
        });
    });
    
    return mappings;
}

/**
 * Normalize column name for comparison
 * @param {string} columnName - Column name to normalize
 * @returns {string} Normalized column name
 */
function normalizeColumnName(columnName) {
    return columnName.toLowerCase()
        .replace(/[_\s-]/g, '') // Remove underscores, spaces, hyphens
        .replace(/^(customer|user|account|contact)/i, ''); // Remove common prefixes
}

/**
 * Add a mapping row to the UI
 * @param {string} sourceColumn - Source column from CSV
 * @param {string} targetField - Target field to map to
 */
function addMappingRow(sourceColumn = '', targetField = '') {
    const rowDiv = document.createElement('div');
    rowDiv.className = 'row mb-2 mapping-row';
    
    // Source column dropdown
    const sourceColDiv = document.createElement('div');
    sourceColDiv.className = 'col-5';
    
    const sourceSelect = document.createElement('select');
    sourceSelect.className = 'form-select source-column';
    
    // Add empty option
    const emptyOption = document.createElement('option');
    emptyOption.value = '';
    emptyOption.textContent = '-- Select CSV Column --';
    sourceSelect.appendChild(emptyOption);
    
    // Add options for all available columns
    state.availableSourceColumns.forEach(col => {
        const option = document.createElement('option');
        option.value = col;
        option.textContent = col;
        option.selected = (col === sourceColumn);
        sourceSelect.appendChild(option);
    });
    
    sourceColDiv.appendChild(sourceSelect);
    
    // Arrow icon
    const arrowDiv = document.createElement('div');
    arrowDiv.className = 'col-2 text-center';
    arrowDiv.innerHTML = '<i class="bi bi-arrow-right"></i>';
    
    // Target field dropdown
    const targetFieldDiv = document.createElement('div');
    targetFieldDiv.className = 'col-5';
    
    const targetSelect = document.createElement('select');
    targetSelect.className = 'form-select target-field';
    
    // Add empty option
    const emptyTargetOption = document.createElement('option');
    emptyTargetOption.value = '';
    emptyTargetOption.textContent = '-- Select Target Field --';
    targetSelect.appendChild(emptyTargetOption);
    
    // Add options for all expected fields
    state.expectedTargetFields.forEach(field => {
        const option = document.createElement('option');
        option.value = field;
        option.textContent = field;
        option.selected = (field === targetField);
        targetSelect.appendChild(option);
    });
    
    targetFieldDiv.appendChild(targetSelect);
    
    // Add all elements to the row
    rowDiv.appendChild(sourceColDiv);
    rowDiv.appendChild(arrowDiv);
    rowDiv.appendChild(targetFieldDiv);
    
    // Add the row to the container
    elements.columnMappingsContainer.appendChild(rowDiv);
}

/**
 * Add an empty mapping row
 */
function addEmptyMappingRow() {
    addMappingRow();
}

/**
 * Get current column mappings from the UI
 * @returns {Object} Column mappings
 */
function getColumnMappings() {
    const mappings = {};
    
    document.querySelectorAll('.mapping-row').forEach(row => {
        const sourceSelect = row.querySelector('.source-column');
        const targetSelect = row.querySelector('.target-field');
        
        const sourceValue = sourceSelect.value;
        const targetValue = targetSelect.value;
        
        if (sourceValue && targetValue) {
            mappings[sourceValue] = targetValue;
        }
    });
    
    return mappings;
}

/**
 * Add Apply Mappings button to the UI
 */
function addApplyMappingsButton() {
    // Check if button already exists
    if (document.getElementById('apply-mappings-btn')) {
        return;
    }
    
    const mappingFooter = document.createElement('div');
    mappingFooter.className = 'card-footer text-end';
    
    const applyButton = document.createElement('button');
    applyButton.id = 'apply-mappings-btn';
    applyButton.className = 'btn btn-primary';
    applyButton.textContent = 'Apply Mappings & Re-Upload';
    applyButton.addEventListener('click', handleApplyMappings);
    
    mappingFooter.appendChild(applyButton);
    elements.columnMappingCard.appendChild(mappingFooter);
}

/**
 * Handle Apply Mappings button click
 */
function handleApplyMappings() {
    // Get current mappings
    const columnMappings = getColumnMappings();
    console.log("Applying column mappings:", columnMappings);
    
    // Check if we still have the file in the input
    if (!elements.fileInput.files.length) {
        showError('Please select a file first');
        return;
    }
    
    // Re-upload the file with the mappings
    const file = elements.fileInput.files[0];
    const formData = new FormData();
    formData.append('file', file);
    
    // Get all checked required columns
    const requiredColumns = [];
    document.querySelectorAll('.required-column:checked').forEach(function(checkbox) {
        requiredColumns.push(checkbox.value.trim());
    });
    
    // Add required columns as a JSON array string
    formData.append('required_columns', JSON.stringify(requiredColumns));
    
    // Add column mappings
    formData.append('column_mappings', JSON.stringify(columnMappings));
    
    // Show loading indicator
    elements.loadingIndicator.classList.remove('d-none');
    
    // Send file to server again with mappings
    fetch('/upload', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) {
            return response.json().then(data => {
                throw new Error(JSON.stringify(data));
            });
        }
        return response.json();
    })
    .then(data => {
        // Hide loading indicator
        elements.loadingIndicator.classList.add('d-none');
        
        // Store file data for later validation
        state.fileData = data;
        
        // Show success message
        showSuccess("File uploaded with column mappings applied!");
        
        // Update file info display
        elements.fileInfo.innerHTML = `
            <div class="alert alert-info">
                <h5>File uploaded successfully with mappings</h5>
                <p>File: <strong>${file.name}</strong></p>
                <p>Total Rows: <strong>${data.rows}</strong></p>
                <p>Detected Delimiter: <strong>${data.detected_delimiter === '\t' ? 'Tab' : data.detected_delimiter}</strong></p>
                <p>Detected Encoding: <strong>${data.detected_encoding}</strong></p>
                <p>Required Columns:</p>
                <ul>
                    ${requiredColumns.map(col => `<li>${col}</li>`).join('')}
                </ul>
            </div>
        `;
        
        // Enable validate button
        elements.validateButton.disabled = false;
        
        // Update preview table
        displayPreviewTable(data);
    })
    .catch(error => {
        elements.loadingIndicator.classList.add('d-none');
        console.error('Upload error:', error);
        
        try {
            // Try to parse the error data
            const errorData = JSON.parse(error.message);
            
            // If we still have missing columns after applying mappings
            if (errorData.error && errorData.missing_columns) {
                showError(`Some columns are still missing after applying mappings: ${errorData.missing_columns.join(', ')}`);
            } else {
                showError(errorData.error || 'Error uploading file. Please try again.');
            }
        } catch (e) {
            // If parsing fails, show the original error
            showError(error.message || 'Error uploading file. Please try again.');
        }
    });
}

// -------------------------
// Validation Handling
// -------------------------

/**
 * Handle validation button click
 */
function handleValidation() {
    console.log("Validate button clicked, fileData:", state.fileData);
    
    // Check if we have file data
    if (!state.fileData || !state.fileData.file_id) {
        showError('No file to validate. Please upload a file first.');
        return;
    }
    
    // Show loading indicator
    elements.loadingIndicator.classList.remove('d-none');
    
    // Clear previous validation results
    elements.validationResults.innerHTML = '';
    
    // Get current column mappings
    const columnMappings = getColumnMappings();
    
    // Get all checked required columns
    const requiredColumns = [];
    document.querySelectorAll('.required-column:checked').forEach(function(checkbox) {
        requiredColumns.push(checkbox.value.trim());
    });
    
    // Send validation request
    fetch('/validate', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            'file_id': state.fileData.file_id,
            'required_columns': requiredColumns,
            'column_mappings': columnMappings
        })
    })
    .then(response => {
        if (!response.ok) {
            return response.json().then(data => {
                throw new Error(JSON.stringify(data));
            });
        }
        return response.json();
    })
    .then(data => {
        // Hide loading indicator
        elements.loadingIndicator.classList.add('d-none');
        
        // Display validation results
        displayValidationResults(data);
    })
    .catch(error => {
        elements.loadingIndicator.classList.add('d-none');
        console.error('Validation error:', error);
        
        try {
            // Try to parse the error data
            const errorData = JSON.parse(error.message);
            showError(errorData.error || 'Error during validation. Please try again.');
        } catch (e) {
            // If parsing fails, show the original error
            showError(error.message || 'Error during validation. Please try again.');
        }
    });
}


/**
 * Display validation results
 * @param {Object} data - Validation results from server
 */
function displayValidationResults(data) {
    // Create validation summary
    const validationSummary = document.createElement('div');
    validationSummary.className = 'validation-summary';
   
    // Add summary statistics
    validationSummary.innerHTML = `
        <div class="alert ${data.invalid_rows > 0 ? 'alert-warning' : 'alert-success'}">
            <h4>Validation Summary</h4>
            <p>Total Rows: <strong>${data.total_rows}</strong></p>
            <p>Valid Rows: <strong>${data.valid_rows}</strong></p>
            <p>Invalid Rows: <strong>${data.invalid_rows}</strong></p>
        </div>
    `;
   
    // Add error counts if there are any
    if (Object.keys(data.error_counts).length > 0) {
        // Check if there are duplicate email errors
        const hasDuplicateEmails = Object.keys(data.error_counts).some(
            error => error.toLowerCase().includes('duplicate email')
        );
        
        let errorCountsHTML = `
            <div class="card mb-4">
                <div class="card-header bg-danger text-white">
                    <h5 class="mb-0">Error Counts</h5>
                </div>
                <div class="card-body">
        `;
        
        // Add warning for duplicate emails if found
        if (hasDuplicateEmails) {
            errorCountsHTML += `
                <div class="alert alert-warning">
                    <strong>Duplicate emails detected!</strong> These are highlighted in the errors below.
                </div>
            `;
        }
        
        errorCountsHTML += `
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Error Type</th>
                                <th>Count</th>
                            </tr>
                        </thead>
                        <tbody>
        `;
        
        // Add rows for each error type
        Object.entries(data.error_counts).forEach(([error, count]) => {
            const isDuplicateEmail = error.toLowerCase().includes('duplicate email');
            errorCountsHTML += `
                <tr ${isDuplicateEmail ? 'class="table-warning"' : ''}>
                    <td>${error}</td>
                    <td>${count}</td>
                </tr>
            `;
        });
        
        errorCountsHTML += `
                        </tbody>
                    </table>
                </div>
            </div>
        `;
        
        validationSummary.innerHTML += errorCountsHTML;
    }
   
    // Add detailed results if there are any invalid rows
    if (data.invalid_rows > 0) {
        // Filter to only show invalid rows
        const invalidResults = data.results.filter(result => !result.valid);
       
        let invalidRowsHTML = `
            <div class="card">
                <div class="card-header bg-warning">
                    <h5 class="mb-0">Invalid Rows (showing ${invalidResults.length} of ${data.invalid_rows})</h5>
                </div>
                <div class="card-body">
                    <div class="table-responsive">
                        <table class="table table-striped table-bordered">
                            <thead>
                                <tr>
                                    <th>Row</th>
                                    <th>Code</th>
                                    <th>Errors</th>
                                </tr>
                            </thead>
                            <tbody>
        `;
        
        // Add rows for each invalid result
        invalidResults.forEach(result => {
            invalidRowsHTML += `
                <tr>
                    <td>${result.row}</td>
                    <td>${result.code}</td>
                    <td>
                        <ul class="mb-0">
            `;
            
            // Add list items for each error
            result.errors.forEach(error => {
                const isDuplicateEmail = error.toLowerCase().includes('duplicate email');
                invalidRowsHTML += `
                    <li ${isDuplicateEmail ? 'class="text-danger fw-bold"' : ''}>${error}</li>
                `;
            });
            
            // Close the list, cell, and row
            invalidRowsHTML += `
                        </ul>
                    </td>
                </tr>
            `;
        });
        
        // Close the table and card
        invalidRowsHTML += `
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
        
        validationSummary.innerHTML += invalidRowsHTML;
    }
   
    // Add the validation summary to the results container
    elements.validationResults.appendChild(validationSummary);
   
    // Scroll to results
    elements.validationResults.scrollIntoView({ behavior: 'smooth' });
}



/**
 * Show error message
 * @param {string} message - Error message to display
 */
function showError(message) {
    const errorAlert = document.createElement('div');
    errorAlert.className = 'alert alert-danger alert-dismissible fade show';
    errorAlert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Clear previous errors
    elements.errorContainer.innerHTML = '';
    
    // Add new error
    elements.errorContainer.appendChild(errorAlert);
    
    // Scroll to error
    elements.errorContainer.scrollIntoView({ behavior: 'smooth' });
    
    // Auto-dismiss after 10 seconds
    setTimeout(() => {
        errorAlert.remove();
    }, 10000);
}

/**
 * Show success message
 * @param {string} message - Success message to display
 */
function showSuccess(message) {
    const successAlert = document.createElement('div');
    successAlert.className = 'alert alert-success alert-dismissible fade show';
    successAlert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Add to error container (reusing it for all alerts)
    elements.errorContainer.appendChild(successAlert);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        successAlert.remove();
    }, 5000);
}
