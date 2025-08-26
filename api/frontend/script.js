// This script handles the frontend logic for submitting jobs and polling their status.
document.addEventListener('DOMContentLoaded', () => {

    // Get references to all the HTML elements we will interact with.
    const form = document.getElementById('task-form');
    const selectTask = document.getElementById('task-name');
    const inputParams = document.getElementById('task-params');
    const submitButton = document.getElementById('submit-button');
    const buttonText = document.getElementById('button-text');
    const buttonSpinner = document.getElementById('button-spinner');
    const statusDisplay = document.getElementById('status-display');

    selectTask.addEventListener('change', () => {
        if (selectTask.value === 'scan_url') {
            inputParams.placeholder = 'https://example.com';
        } else if (selectTask.value === 'fetch_ip') {
            inputParams.placeholder = 'google.com';
        }
    });

    // --- Form Submission Handler ---
    form.addEventListener('submit', async (event) => {
        event.preventDefault(); 

        // --- UI Feedback: Show that we're working ---
        submitButton.disabled = true;
        buttonText.textContent = 'Submitting...';
        buttonSpinner.classList.remove('d-none'); 

        const taskName = selectTask.value;
        const paramValue = inputParams.value;

        
        const params = (taskName === 'scan_url') ? { url: paramValue } : { hostname: paramValue };

        try {
            // --- Step 1: Submit the job to the API ---
            const response = await fetch('/tasks', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ task_name: taskName, params: params }),
            });

            if (!response.ok) {
                // If the API returns an error (e.g., 4xx, 5xx), throw an error to be caught below.
                const errorData = await response.json();
                throw new Error(errorData.detail || `HTTP error! Status: ${response.status}`);
            }

            const data = await response.json();
            const taskId = data.task_id;

            // --- Step 2: If submission is successful, start polling for the status ---
            updateStatus('info', `<i class="bi bi-hourglass-split me-2"></i>Job submitted! Task ID: <strong>${taskId}</strong>. Waiting for a worker to pick it up...`);
            pollForStatus(taskId);

        } catch (error) {
            // If anything goes wrong during submission, show an error.
            updateStatus('danger', `<i class="bi bi-exclamation-triangle-fill me-2"></i><strong>Error:</strong> ${error.message}`);
            resetButton(); // Reset the button state on failure.
        }
    });

    // --- The Polling Function ---
    function pollForStatus(taskId) {
        const intervalId = setInterval(async () => {
            try {
                const response = await fetch(`/tasks/${taskId}`);
                
                
                if (response.status === 404) {
                    clearInterval(intervalId);
                    updateStatus('danger', `<i class="bi bi-exclamation-triangle-fill me-2"></i><strong>Error:</strong> Task with ID <strong>${taskId}</strong> not found. It may have been cleared.`);
                    resetButton();
                    return;
                }

                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`);
                }

                const result = await response.json();
                const status = result.status.toUpperCase();
                
                let statusMessage = `<b>Task ID:</b> ${result.id}<br><b>Status:</b> ${status}`;

                if (status === 'PENDING') {
                    updateStatus('info', `<i class="bi bi-hourglass-split me-2"></i>${statusMessage}`);
                } else if (status === 'IN_PROGRESS') {
                    updateStatus('warning', `<i class="bi bi-gear-fill me-2"></i>${statusMessage}`);
                } else {
                    
                    clearInterval(intervalId);
                    resetButton(); 

                    if (status === 'COMPLETED') {
                        
                        statusMessage += `<br><b>Result:</b> <pre>${JSON.stringify(result.result, null, 2)}</pre>`;
                        updateStatus('success', `<i class="bi bi-check-circle-fill me-2"></i>${statusMessage}`);
                    } else {
                        statusMessage += `<br><b>Error Details:</b> <pre>${JSON.stringify(result.result, null, 2)}</pre>`;
                        updateStatus('danger', `<i class="bi bi-exclamation-triangle-fill me-2"></i>${statusMessage}`);
                    }
                }
            } catch (error) {
                
                clearInterval(intervalId);
                updateStatus('danger', `<i class="bi bi-exclamation-triangle-fill me-2"></i><strong>Polling Error:</strong> ${error.message}`);
                resetButton();
            }
        }, 2000); // Poll the API every 2 seconds.
    }

    
    function updateStatus(type, htmlMessage) {
        statusDisplay.className = `alert alert-${type}`; 
        statusDisplay.innerHTML = htmlMessage;
    }

    
    function resetButton() {
        submitButton.disabled = false;
        buttonText.textContent = 'Submit Job';
        buttonSpinner.classList.add('d-none'); 
    }
});