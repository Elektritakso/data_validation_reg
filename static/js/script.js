
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
    ]
};


document.addEventListener('DOMContentLoaded', () => {
    
    initEventListeners();
});

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
    
    
    if (!elements.fileInput.files.length) {
        showError('Please select a file first');
        return;
    }
    
    const file = elements.fileInput.files[0];
    const formData = new FormData();
    formData.append('file', file);
    
    
    const requiredColumns = [];
    document.querySelectorAll('.required-column:checked').forEach(function(checkbox) {
        requiredColumns.push(checkbox.value.trim());
    });
    
    formData.append('required_columns', JSON.stringify(requiredColumns));
    
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
    
    const suggestedMappings = suggestMappings(csvColumns, state.expectedTargetFields);
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
        state.expectedTargetFields.forEach(field => {
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
    
    if (!elements.fileInput.files.length) {
        showError('Please select a file first');
        return;
    }
    
    const file = elements.fileInput.files[0];
    const formData = new FormData();
    formData.append('file', file);
    
    const requiredColumns = [];
    document.querySelectorAll('.required-column:checked').forEach(function(checkbox) {
        requiredColumns.push(checkbox.value.trim());
    });
    
    formData.append('required_columns', JSON.stringify(requiredColumns));
    
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
        if (!state.fileData || !state.fileData.file_id) {
        showError('No file to validate. Please upload a file first.');
        return;
    }
    
    elements.loadingIndicator.classList.remove('d-none');
    
    elements.validationResults.innerHTML = '';
    
    const columnMappings = getColumnMappings();
    
    const requiredColumns = [];
    document.querySelectorAll('.required-column:checked').forEach(function(checkbox) {
        requiredColumns.push(checkbox.value.trim());
    });
    






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
       validationSummary.innerHTML = `
        <div class="alert ${data.invalid_rows > 0 ? 'alert-warning' : 'alert-success'}">
            <h4>Validation Summary</h4>
            <p>Total Rows: <strong>${data.total_rows}</strong></p>
            <p>Valid Rows: <strong>${data.valid_rows}</strong></p>
            <p>Invalid Rows: <strong>${data.invalid_rows}</strong></p>
        </div>
    `;
   
    if (Object.keys(data.error_counts).length > 0) {
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
   
    if (data.invalid_rows > 0) {
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
