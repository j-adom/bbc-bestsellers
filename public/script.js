document.getElementById('process-button').addEventListener('click', async () => {
    const resultDiv = document.getElementById('result');
    resultDiv.textContent = 'Processing... This may take a while.';

    try {
        const response = await fetch('/process', { method: 'POST' });
        const result = await response.json();
        
        resultDiv.innerHTML = `
            <p>${result.message}</p>
            <a href="${result.downloadUrl}" download="processed_data.csv">Download processed file</a>
        `;
    } catch (error) {
        resultDiv.textContent = `Error: ${error.message}`;
    }
});