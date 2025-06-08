/**
 * Simple regulation selector functionality
 */
document.addEventListener('DOMContentLoaded', function() {
    // Create regulation selector container if it doesn't exist
    let container = document.getElementById('regulation-selector-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'regulation-selector-container';
        
        // Insert before the upload form
        const uploadForm = document.getElementById('upload-form');
        if (uploadForm) {
            uploadForm.parentNode.insertBefore(container, uploadForm);
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
});

function createRegulationSelector(container, regulations) {
    let html = `
        <div class="card mb-4">
            <div class="card-header">
                <h5>Select Regulation</h5>
            </div>
            <div class="card-body">
                <select id="regulation-selector" class="form-select">
                    <option value="">-- Select a Regulation --</option>`;
                    
    for (const [code, name] of Object.entries(regulations)) {
        html += `<option value="${code}">${name}</option>`;
    }
    
    html += `
                </select>
                <div class="form-text">
                    Select the regulatory framework to validate against
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
    
    // Add event listener to the selector
    const selector = document.getElementById('regulation-selector');
    if (selector) {
        selector.addEventListener('change', function(e) {
            window.selectedRegulation = e.target.value;
            console.log(`Selected regulation: ${window.selectedRegulation}`);
            
            // Update the displayed fields based on the selected regulation
            if (window.selectedRegulation) {
                updateRequiredFieldsDisplay(window.selectedRegulation);
            } else {
                // If no regulation is selected, show all fields
                resetFieldsDisplay();
            }
        });
    }
}

// Function to update the displayed fields based on selected regulation
function updateRequiredFieldsDisplay(regulationCode) {
    fetch(`/regulation-fields/${regulationCode}`)
        .then(response => response.json())
        .then(data => {
            if (data.required_fields) {
                // Store the required fields for later use
                window.requiredFields = data.required_fields;
                
                // Update the column mapping UI to show only required fields
                updateColumnMappingUI(data.required_fields);
                
                console.log(`Required fields for ${regulationCode}:`, data.required_fields);
            }
        })
        .catch(error => {
            console.error('Error fetching regulation fields:', error);
        });
}

// Function to update the column mapping UI
function updateColumnMappingUI(requiredFields) {
    const columnMappingsContainer = document.getElementById('column-mappings-container');
    if (!columnMappingsContainer) return;
    
    // Clear existing mappings
    columnMappingsContainer.innerHTML = '';
    
    // Create mapping rows only for required fields
    requiredFields.forEach(field => {
        addMappingRow(columnMappingsContainer, field);
    });
}

// Function to add a mapping row for a specific field
function addMappingRow(container, targetField) {
    const sourceOptions = window.state && window.state.availableSourceColumns ? 
        window.state.availableSourceColumns : [];
    
    const row = document.createElement('div');
    row.className = 'row mb-2 mapping-row';
    
    row.innerHTML = `
        <div class="col-5">
            <select class="form-select source-column">
                <option value="">-- Select CSV Column --</option>
                ${sourceOptions.map(col => `<option value="${col}">${col}</option>`).join('')}
            </select>
        </div>
        <div class="col-1 text-center">
            <span class="align-middle">→</span>
        </div>
        <div class="col-5">
            <input type="text" class="form-control target-field" value="${targetField}" readonly>
        </div>
        <div class="col-1">
            <button type="button" class="btn btn-sm btn-outline-danger remove-mapping">×</button>
        </div>
    `;
    
    // Add event listener to remove button
    const removeButton = row.querySelector('.remove-mapping');
    removeButton.addEventListener('click', function() {
        row.remove();
    });
    
    container.appendChild(row);
}

// Function to reset the column mapping UI to show all fields
function resetFieldsDisplay() {
    // If there's a state object with expected target fields
    if (window.state && window.state.expectedTargetFields) {
        updateColumnMappingUI(window.state.expectedTargetFields);
    }
}

// Modify the existing handleValidation function to include regulation
const originalHandleValidation = window.handleValidation;
window.handleValidation = function() {
    // Get the column mappings
    const mappings = {};
    document.querySelectorAll('.mapping-row').forEach(row => {
        const sourceColumn = row.querySelector('.source-column').value;
        const targetField = row.querySelector('.target-field').value;
        
        if (sourceColumn && targetField) {
            mappings[sourceColumn] = targetField;
        }
    });
    
    // Prepare the request data
    const requestData = {
        column_mappings: mappings
    };
    
    // Add regulation if selected
    if (window.selectedRegulation) {
        requestData.regulation = window.selectedRegulation;
    }
    
    // Add required fields if available
    if (window.requiredFields) {
        requestData.required_columns = window.requiredFields;
    }
    
    // Show loading indicator
    const loadingIndicator = document.getElementById('loading-indicator');
    if (loadingIndicator) loadingIndicator.style.display = 'block';
    
    const validationResults = document.getElementById('validation-results');
    if (validationResults) validationResults.innerHTML = '';
    
    // Make the API request
    fetch('/validate', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestData)
    })
    .then(response => response.json())
    .then(data => {
        // Handle the response
        if (window.displayValidationResults) {
            window.displayValidationResults(data);
        } else {
            console.log('Validation results:', data);
        }
    })
    .catch(error => {
        console.error(`Validation failed: ${error.message}`);
    })
    .finally(() => {
        if (loadingIndicator) loadingIndicator.style.display = 'none';
    });
};
