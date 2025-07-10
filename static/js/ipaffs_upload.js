document.addEventListener('DOMContentLoaded', function() {
    // DOM Elements
    const excelModeBtn = document.getElementById('excelModeBtn');
    const pdfModeBtn = document.getElementById('pdfModeBtn');
    const ipaffsModeBtn = document.getElementById('ipaffsModeBtn');
    const ipaffsPdfForm = document.getElementById('ipaffsPdfForm');
    const extractDataBtn = document.getElementById('extractDataBtn');
    const pdfSpinner = document.getElementById('pdfSpinner');
    const errorMessage = document.getElementById('errorMessage');

    // Mode switching
    excelModeBtn.addEventListener('click', function() {
        window.location.href = '/';
    });

    pdfModeBtn.addEventListener('click', function() {
        window.location.href = '/pdf_upload';
    });

    // Handle IPAFFS PDF form submission
    ipaffsPdfForm.addEventListener('submit', function(e) {
        e.preventDefault();
        
        // Hide any previous error
        hideError();
        
        // Show loading spinner
        pdfSpinner.classList.remove('d-none');
        extractDataBtn.disabled = true;
        
        // Get form data
        const formData = new FormData(ipaffsPdfForm);
        
        // Send request to extract data from PDF using IPAFFS schema
        fetch('/extract_ipaffs_pdf', {
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
