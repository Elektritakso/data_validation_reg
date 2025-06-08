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

let state = {
    fileData: null,
    availableSourceColumns: [],
    expectedTargetFields: [
        'code', 'username','firstname', 'lastname', 'email', 'birthdate', 
        'address', 'city', 'phone', 'cellphone', 'countrycode', 
        'signuplanguagecode', 'currencycode', 'zip', 'signupdate'
    ],
    selectedRegulation: null,
    requiredFields: null
};

document.addEventListener('DOMContentLoaded', () => {
    // Initialize regulation selector first
    initRegulationSelector();
    
    // Initialize other event listeners
    initEventListeners();
});

function initRegulationSelector() {
    // Create regulation selector container if it doesn't exist
    let container = document.getElementById('regulation-selector-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'regulation-selector-container';
        container.className = 'mb-4';
        
        // Insert at the top of the main content area
        const mainContent = document.querySelector('.container');
        if (mainContent && mainContent.firstChild) {
            mainContent.insertBefore(container, mainContent.firstChild);
        } else {
            document.body.insertBefore(container, document.body.firstChild);
        }
    }
    
    // Fetch regulations
    fetch('/regulations')
        .then(response => response.json())
        .then(data => {
            if (data.regulations) {
                createRegulationSelector(container, data.regulations);
            }
        })
        .catch(error => {
            console.error('Error loading regulations:', error);
        });
}

function createRegulationSelector(container, regulations) {
    let html = `
        <div class="card">
            <div class="card-header bg-primary text-white">
                <h5 class="mb-0"> Select Regulation </h5>
            </div>
            <div class="card-body">
                <div class="form-group">
                    <select id="regulation-selector" class="form-select">
                        <option value="">-- Select a Regulation --</option>`;
                    
    for (const [code, name] of Object.entries(regulations)) {
        html += `<option value="${code}">${name}</option>`;
    }
    
    html += `
                    </select>
                    <div class="form-text mt-2">
                    </div>
                </div>
            </div>
        </div>
        
        <div id="required-fields-container" class="mt-3" style="display: none;"></div>
    `;
    
    container.innerHTML = html;
    
    // Add event listener to the selector
    const selector = document.getElementById('regulation-selector');
    if (selector) {
        selector.addEventListener('change', function(e) {
            state.selectedRegulation = e.target.value;
            console.log(`Selected regulation: ${state.selectedRegulation}`);
            
            if (state.selectedRegulation) {
                // Show the upload form only after regulation is selected
                showRequiredFields(state.selectedRegulation);
                
                // Enable the upload form
                if (elements.uploadForm) {
                    elements.uploadForm.style.display = 'block';
                }
            } else {
                // Hide the required fields and upload form if no regulation is selected
                document.getElementById('required-fields-container').style.display = 'none';
                
                if (elements.uploadForm) {
                    elements.uploadForm.style.display = 'none';
                }
                
                state.requiredFields = null;
            }
        });
    }
    
    // Initially hide the upload form until a regulation is selected
    if (elements.uploadForm) {
        elements.uploadForm.style.display = 'none';
    }
}

function showRequiredFields(regulationCode) {
    const container = document.getElementById('required-fields-container');
    if (!container) return;
    
    container.innerHTML = '<div class="text-center"><div class="spinner-border" role="status"><span class="visually-hidden">Loading...</span></div></div>';
    container.style.display = 'block';
    
    fetch(`/regulation-fields/${regulationCode}`)
        .then(response => response.json())
        .then(data => {
            if (data.required_fields && data.required_fields.length > 0) {
                // Store the required fields for later use
                state.requiredFields = data.required_fields;
                
                let html = `
                    <div class="card">
                        <div class="card-header bg-info text-white">
                            <h5 class="mb-0">Required Fields for ${data.name}</h5>
                        </div>
                        <div class="card-body">
                            <p>The following fields are required by this regulation:</p>
                            <ul class="list-group">
                `;
                
                data.required_fields.forEach(field => {
                    html += `<li class="list-group-item">${field}</li>`;
                });
                
                html += `
                            </ul>
                            <div class="alert alert-info mt-3">
                                <i class="bi bi-info-circle"></i> 
                                Please upload your CSV file below. The system will validate that all required fields are present.
                            </div>
                        </div>
                    </div>
                `;
                
                container.innerHTML = html;
                
                // Update the upload form title to reflect the next step
                const uploadFormTitle = document.querySelector('#upload-form .card-header h5');
                if (uploadFormTitle) {
                    uploadFormTitle.textContent = 'Step 2: Upload Your CSV File';
                }
                
                console.log(`Required fields for ${regulationCode}:`, data.required_fields);
            } else {
                container.innerHTML = `
                    <div class="alert alert-warning">
                        <i class="bi bi-exclamation-triangle"></i>
                        No required fields defined for this regulation.
                    </div>
                `;
            }
        })
        .catch(error => {
            console.error('Error fetching regulation fields:', error);
            container.innerHTML = `
                <div class="alert alert-danger">
                    <i class="bi bi-exclamation-triangle"></i>
                    Error loading regulation requirements: ${error.message}
                </div>
            `;
        });
}

function initEventListeners() {
    elements.uploadForm.addEventListener('submit', handleFileUpload);
    elements.validateButton.addEventListener('click', handleValidation);
    
    const addMappingButton = document.getElementById('add-mapping-button');
    if (addMappingButton) {
        addMappingButton.addEventListener('click', addEmptyMappingRow);
    }
}

/**
 * @param {Event} event - Form submission event
 */
function handleFileUpload(event) {
    event.preventDefault();
    
    if (!state.selectedRegulation) {
        showError('Please select a regulation first');
        return;
    }
    
    if (!elements.fileInput.files.length) {
        showError('Please select a file first');
        return;
    }
    
    const file = elements.fileInput.files[0];
    const formData = new FormData();
    formData.append('file', file);
    
    // Use required fields from selected regulation
    const requiredColumns = state.requiredFields || [];
    formData.append('required_columns', JSON.stringify(requiredColumns));
    
    // Add the selected regulation to the form data
    formData.append('regulation', state.selectedRegulation);
    
    elements.loadingIndicator.classList.remove('d-none');
    
    elements.fileInfo.innerHTML = '';
    elements.previewTable.innerHTML = '';
    elements.validationResults.innerHTML = '';
    
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
        elements.loadingIndicator.classList.add('d-none');
        
        state.fileData = data;
        
        displayFileInfo(data, file.name, requiredColumns);
        
        displayPreviewTable(data);
        
        elements.validateButton.disabled = false;
    })
    .catch(error => {
        elements.loadingIndicator.classList.add('d-none');
        console.error('Upload error:', error);
        
        try {
            const errorData = JSON.parse(error.message);
            
            if (errorData.error && errorData.missing_columns && errorData.available_columns) {
                showError(`Required columns missing: ${errorData.missing_columns.join(', ')}`);
                showMappingUI(errorData.available_columns, errorData.missing_columns);
            } else {
                showError(errorData.error || 'Error uploading file. Please try again.');
            }
        } catch (e) {
            showError(error.message || 'Error uploading file. Please try again.');
        }
    });
}

/**
 * @param {Object} data
 * @param {string} fileName
 * @param {Array} requiredColumns
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
 * @param {Object} data
 */
function displayPreviewTable(data) {
    let tableHTML = '<div class="table-responsive"><table class="table table-striped table-bordered">';
    tableHTML += '<thead><tr>';
    data.columns.forEach(column => {
        tableHTML += `<th>${column}</th>`;
    });
    tableHTML += '</tr></thead>';
    tableHTML += '<tbody>';
    data.data.forEach(row => {
        tableHTML += '<tr>';
        data.columns.forEach(column => {
            tableHTML += `<td>${row[column] || ''}</td>`;
        });
        tableHTML += '</tr>';
    });
    tableHTML += '</tbody></table></div>';
    elements.previewTable.innerHTML = tableHTML;
}

/**
 * @param {Array} csvColumns
 * @param {Array} missingColumns
 */
function showMappingUI(csvColumns, missingColumns = []) {
    console.log("Showing mapping UI for columns:", csvColumns);
    console.log("Missing columns:", missingColumns);
    
    state.availableSourceColumns = csvColumns;
    
    elements.columnMappingsContainer.innerHTML = '';
    
    // If we have a selected regulation, only show mappings for required fields
    const targetFields = state.requiredFields || state.expectedTargetFields;
    
    const suggestedMappings = suggestMappings(csvColumns, targetFields);
    console.log("Suggested mappings:", suggestedMappings);
    Object.entries(suggestedMappings).forEach(([source, target]) => {
        addMappingRow(source, target);
    });
    
    elements.columnMappingCard.style.display = 'block';
    
    addApplyMappingsButton();
    
    elements.validateButton.disabled = true;
}

/**
 * @param {Array} sourceColumns
 * @param {Array} targetFields
 * @returns {Object}
 */
function suggestMappings(sourceColumns, targetFields) {
    const mappings = {};
    const sourceNormalized = sourceColumns.map(col => normalizeColumnName(col));
    
    sourceColumns.forEach((sourceCol, index) => {
        const normalizedSource = sourceNormalized[index];
        
        targetFields.forEach(targetField => {
            const normalizedTarget = normalizeColumnName(targetField);
            
            if (normalizedSource === normalizedTarget) {
                mappings[sourceCol] = targetField;
            }
        });
    });
    
    sourceColumns.forEach((sourceCol) => {
        if (mappings[sourceCol]) return;
        
        const normalizedSource = normalizeColumnName(sourceCol);
        
        targetFields.forEach(targetField => {
            if (Object.values(mappings).includes(targetField)) return;
            
            const normalizedTarget = normalizeColumnName(targetField);
            if (normalizedSource.includes(normalizedTarget) || normalizedTarget.includes(normalizedSource)) {
                mappings[sourceCol] = targetField;
            }
        });
    });
    
    return mappings;
}

/**
 * @param {string} columnName
 * @returns {string}
 */
function normalizeColumnName(columnName) {
    return columnName.toLowerCase()
        .replace(/[_\s-]/g, '')
        .replace(/^(customer|user|account|contact)/i, '');
}

/**
 * @param {string} sourceColumn
 * @param {string} targetField
 */
function addMappingRow(sourceColumn = '', targetField = '') {
    const rowDiv = document.createElement('div');
    rowDiv.className = 'row mb-2 mapping-row';
    
    const sourceColDiv = document.createElement('div');
    sourceColDiv.className = 'col-5';
    
    const sourceSelect = document.createElement('select');
    sourceSelect.className = 'form-select source-column';
    
    const emptyOption = document.createElement('option');
    emptyOption.value = '';
    emptyOption.textContent = '-- Select CSV Column --';
    sourceSelect.appendChild(emptyOption);
    
    state.availableSourceColumns.forEach(col => {
        const option = document.createElement('option');
        option.value = col;
        option.textContent = col;
        option.selected = (col === sourceColumn);
        sourceSelect.appendChild(option);
    });
    
    sourceColDiv.appendChild(sourceSelect);
    
    const arrowDiv = document.createElement('div');
    arrowDiv.className = 'col-2 text-center';
    arrowDiv.innerHTML = '<i class="bi bi-arrow-right"></i>';
    
    const targetFieldDiv = document.createElement('div');
    targetFieldDiv.className = 'col-5';
    
    const targetSelect = document.createElement('select');
    targetSelect.className = 'form-select target-field';
    
    const emptyTargetOption = document.createElement('option');
    emptyTargetOption.value = '';
    emptyTargetOption.textContent = '-- Select Target Field --';
    targetSelect.appendChild(emptyTargetOption);
    
    // Use required fields from regulation if available
    const targetFields = state.requiredFields || state.expectedTargetFields;
    
    targetFields.forEach(field => {
        const option = document.createElement('option');
        option.value = field;
        option.textContent = field;
        option.selected = (field === targetField);
        targetSelect.appendChild(option);
    });
    
    targetFieldDiv.appendChild(targetSelect);
    
    rowDiv.appendChild(sourceColDiv);
    rowDiv.appendChild(arrowDiv);
    rowDiv.appendChild(targetFieldDiv);
    
    elements.columnMappingsContainer.appendChild(rowDiv);
}

function addEmptyMappingRow() {
    addMappingRow();
}

/**
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

function addApplyMappingsButton() {
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

function handleApplyMappings() {
    const columnMappings = getColumnMappings();
    console.log("Applying column mappings:", columnMappings);
    
    if (!state.selectedRegulation) {
        showError('Please select a regulation first');
        return;
    }
    
    if (!elements.fileInput.files.length) {
        showError('Please select a file first');
        return;
    }
    
    const file = elements.fileInput.files[0];
    const formData = new FormData();
    formData.append('file', file);
    
    // Use required fields from regulation if available
    const requiredColumns = state.requiredFields || [];
    formData.append('required_columns', JSON.stringify(requiredColumns));
    
    // Add the selected regulation to the form data
    formData.append('regulation', state.selectedRegulation);
    
    formData.append('column_mappings', JSON.stringify(columnMappings));
    
    elements.loadingIndicator.classList.remove('d-none');
    
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
        elements.loadingIndicator.classList.add('d-none');
        
        state.fileData = data;
        
        showSuccess("File uploaded with column mappings applied!");
        
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
        
        elements.validateButton.disabled = false;
        
        displayPreviewTable(data);
    })
    .catch(error => {
        elements.loadingIndicator.classList.add('d-none');
        console.error('Upload error:', error);
        
        try {
            const errorData = JSON.parse(error.message);
            
            if (errorData.error && errorData.missing_columns) {
                showError(`Some columns are still missing after applying mappings: ${errorData.missing_columns.join(', ')}`);
            } else {
                showError(errorData.error || 'Error uploading file. Please try again.');
            }
        } catch (e) {
            showError(error.message || 'Error uploading file. Please try again.');
        }
    });
}

function handleValidation() {
    console.log("Validate button clicked, fileData:", state.fileData);
    
    if (!state.selectedRegulation) {
        showError('Please select a regulation first');
        return;
    }
    
    if (!state.fileData || !state.fileData.file_id) {
        showError('No file to validate. Please upload a file first.');
        return;
    }
    
    elements.loadingIndicator.classList.remove('d-none');
    
    elements.validationResults.innerHTML = '';
    
    const columnMappings = getColumnMappings();
    
    // Use required fields from regulation if available
    const requiredColumns = state.requiredFields || [];
    
    const requestData = {
        'file_id': state.fileData.file_id,
        'required_columns': requiredColumns,
        'column_mappings': columnMappings,
        'regulation': state.selectedRegulation
    };
    
    fetch('/validate', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestData)
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
        elements.loadingIndicator.classList.add('d-none');
        
        displayValidationResults(data);
    })
    .catch(error => {
        elements.loadingIndicator.classList.add('d-none');
        console.error('Validation error:', error);
        
        try {
            const errorData = JSON.parse(error.message);
            showError(errorData.error || 'Error during validation. Please try again.');
        } catch (e) {
            showError(error.message || 'Error during validation. Please try again.');
        }
    });
}

/**
 * @param {Object} data - Validation results from server
 */
function displayValidationResults(data) {
    const validationSummary = document.createElement('div');
    validationSummary.className = 'validation-summary';
    
    validationSummary.innerHTML = `        <div class="alert ${data.invalid_rows > 0 ? 'alert-warning' : 'alert-success'}">
            <h4 class="alert-heading">Validation Complete</h4>
            <p>Total Rows: <strong>${data.total_rows}</strong></p>
            <p>Valid Rows: <strong>${data.valid_rows}</strong></p>
            <p>Invalid Rows: <strong>${data.invalid_rows}</strong></p>
            ${data.duplicate_email_count > 0 ? `<p>Duplicate Emails: <strong>${data.duplicate_email_count}</strong></p>` : ''}
            ${data.duplicate_username_count > 0 ? `<p>Duplicate Usernames: <strong>${data.duplicate_username_count}</strong></p>` : ''}
        </div>
    `;
    
    if (Object.keys(data.error_counts).length > 0) {
        let errorSummaryHTML = `
            <div class="card mb-4">
                <div class="card-header bg-danger text-white">
                    <h5 class="mb-0">Error Summary</h5>
                </div>
                <div class="card-body">
                    <div class="table-responsive">
                        <table class="table table-striped">
                            <thead>
                                <tr>
                                    <th>Error Type</th>
                                    <th>Count</th>
                                </tr>
                            </thead>
                            <tbody>
        `;
        
        for (const [errorType, count] of Object.entries(data.error_counts)) {
            errorSummaryHTML += `
                <tr>
                    <td>${errorType}</td>
                    <td>${count}</td>
                </tr>
            `;
        }
        
        errorSummaryHTML += `
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
        
        validationSummary.innerHTML += errorSummaryHTML;
    }
    
    if (data.invalid_rows > 0 && data.results) {
        const invalidResults = data.results.filter(result => !result.valid);
        
        let invalidRowsHTML = `
            <div class="card mb-4">
                <div class="card-header bg-warning">
                    <h5 class="mb-0">Invalid Rows (showing first ${Math.min(invalidResults.length, 100)})</h5>
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
        
        invalidResults.forEach(result => {
            invalidRowsHTML += `
                <tr>
                    <td>${result.row}</td>
                    <td>${result.code}</td>
                    <td>
                        <ul class="mb-0">
            `;
            
            result.errors.forEach(error => {
                const isDuplicateEmail = error.toLowerCase().includes('duplicate email');
                invalidRowsHTML += `
                    <li ${isDuplicateEmail ? 'class="text-danger fw-bold"' : ''}>${error}</li>
                `;
            });
            
            invalidRowsHTML += `
                        </ul>
                    </td>
                </tr>
            `;
        });
        
        invalidRowsHTML += `
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
        
        validationSummary.innerHTML += invalidRowsHTML;
    }
    
    elements.validationResults.appendChild(validationSummary);
    
    elements.validationResults.scrollIntoView({ behavior: 'smooth' });
}

/**
 * @param {string} message - Error message to display
 */
function showError(message) {
    const errorAlert = document.createElement('div');
    errorAlert.className = 'alert alert-danger alert-dismissible fade show';
    errorAlert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    elements.errorContainer.innerHTML = '';
    
    elements.errorContainer.appendChild(errorAlert);
    
    elements.errorContainer.scrollIntoView({ behavior: 'smooth' });
    
    setTimeout(() => {
        errorAlert.remove();
    }, 10000);
}

/**
 * @param {string} message - Success message to display
 */
function showSuccess(message) {
    const successAlert = document.createElement('div');
    successAlert.className = 'alert alert-success alert-dismissible fade show';
    successAlert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    elements.errorContainer.appendChild(successAlert);
    
    setTimeout(() => {
        successAlert.remove();
    }, 5000);
}

// Expose functions to window for external access if needed
window.getColumnMappings = getColumnMappings;
window.displayValidationResults = displayValidationResults;
window.addMappingRow = addMappingRow;

