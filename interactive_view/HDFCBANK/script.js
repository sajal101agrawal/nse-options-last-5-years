// Global variables for data storage
let processedData = null;
let dates = [];
let ceIV30 = [];
let ceIV60 = [];
let ceIV90 = [];
let peIV30 = [];
let peIV60 = [];
let peIV90 = [];
let avgIV30 = [];
let avgIV60 = [];
let avgIV90 = [];
let rvYZ = [];
let ivRvSpread = [];
let underlyingPrice = [];

// Load data from CSV file
async function loadData() {
    try {
        const response = await fetch('./analysis/processed_data.csv');
        const csvData = await response.text();
        processData(csvData);
        updateDashboardStats();
        createInteractiveCharts();
    } catch (error) {
        console.error('Error loading data:', error);
    }
}

// Process CSV data
function processData(csvData) {
    const lines = csvData.split('\n');
    const headers = lines[0].split(',');
    
    for (let i = 1; i < lines.length; i++) {
        if (lines[i].trim() === '') continue;
        
        const values = lines[i].split(',');
        const row = {};
        
        for (let j = 0; j < headers.length; j++) {
            row[headers[j]] = values[j];
        }
        
        dates.push(row.date);
        ceIV30.push(parseFloat(row.ce_iv_30) || null);
        ceIV60.push(parseFloat(row.ce_iv_60) || null);
        ceIV90.push(parseFloat(row.ce_iv_90) || null);
        peIV30.push(parseFloat(row.pe_iv_30) || null);
        peIV60.push(parseFloat(row.pe_iv_60) || null);
        peIV90.push(parseFloat(row.pe_iv_90) || null);
        
        const avgIV30Value = (parseFloat(row.ce_iv_30) + parseFloat(row.pe_iv_30)) / 2;
        avgIV30.push(avgIV30Value || null);
        
        const avgIV60Value = (parseFloat(row.ce_iv_60) + parseFloat(row.pe_iv_60)) / 2;
        avgIV60.push(avgIV60Value || null);
        
        const avgIV90Value = (parseFloat(row.ce_iv_90) + parseFloat(row.pe_iv_90)) / 2;
        avgIV90.push(avgIV90Value || null);
        
        const rvValue = parseFloat(row.rv_yz) * 100 || null;
        rvYZ.push(rvValue);
        
        ivRvSpread.push(avgIV30Value - rvValue || null);
        underlyingPrice.push(parseFloat(row.underlying_price) || null);
    }
    
    processedData = {
        dates,
        ceIV30,
        ceIV60,
        ceIV90,
        peIV30,
        peIV60,
        peIV90,
        avgIV30,
        avgIV60,
        avgIV90,
        rvYZ,
        ivRvSpread,
        underlyingPrice
    };
}

// Update dashboard statistics
function updateDashboardStats() {
    // Calculate average values
    const avgIV30Value = avgIV30.reduce((sum, val) => sum + (val || 0), 0) / avgIV30.filter(val => val !== null).length;
    const avgRVValue = rvYZ.reduce((sum, val) => sum + (val || 0), 0) / rvYZ.filter(val => val !== null).length;
    const avgSpreadValue = ivRvSpread.reduce((sum, val) => sum + (val || 0), 0) / ivRvSpread.filter(val => val !== null).length;
    
    // Update dashboard cards
    document.getElementById('data-points-count').textContent = dates.length.toLocaleString();
    document.getElementById('avg-iv-30').textContent = avgIV30Value.toFixed(1) + '%';
    document.getElementById('avg-rv').textContent = avgRVValue.toFixed(1) + '%';
    document.getElementById('iv-rv-spread').textContent = avgSpreadValue.toFixed(1) + '%';
}

// Create interactive charts
function createInteractiveCharts() {
    createTimeSeriesChart();
    createTermStructureChart();
}

// Create interactive time series chart
function createTimeSeriesChart() {
    const ctx = document.getElementById('interactive-iv-rv-timeseries');
    if (!ctx) return;
    
    // Create Plotly chart
    const traces = [
        {
            x: dates,
            y: avgIV30,
            type: 'scatter',
            mode: 'lines',
            name: 'Avg IV 30d',
            line: {
                color: '#3498db',
                width: 2
            }
        },
        {
            x: dates,
            y: avgIV60,
            type: 'scatter',
            mode: 'lines',
            name: 'Avg IV 60d',
            line: {
                color: '#2ecc71',
                width: 2
            }
        },
        {
            x: dates,
            y: avgIV90,
            type: 'scatter',
            mode: 'lines',
            name: 'Avg IV 90d',
            line: {
                color: '#9b59b6',
                width: 2
            }
        },
        {
            x: dates,
            y: rvYZ,
            type: 'scatter',
            mode: 'lines',
            name: 'RV (YZ) %',
            line: {
                color: '#e67e22',
                width: 2,
                dash: 'dash'
            }
        }
    ];
    
    const layout = {
        title: 'Time Series of Implied Volatility vs Realized Volatility',
        xaxis: {
            title: 'Date'
        },
        yaxis: {
            title: 'Volatility (%)'
        },
        legend: {
            orientation: 'h',
            y: -0.2
        },
        margin: {
            l: 50,
            r: 50,
            t: 50,
            b: 100
        }
    };
    
    Plotly.newPlot('interactive-iv-rv-timeseries', traces, layout);
    
    // Add event listeners for checkboxes
    document.getElementById('show-iv-30').addEventListener('change', updateTimeSeriesVisibility);
    document.getElementById('show-iv-60').addEventListener('change', updateTimeSeriesVisibility);
    document.getElementById('show-iv-90').addEventListener('change', updateTimeSeriesVisibility);
    document.getElementById('show-rv').addEventListener('change', updateTimeSeriesVisibility);
}

// Update time series chart visibility based on checkboxes
function updateTimeSeriesVisibility() {
    const showIV30 = document.getElementById('show-iv-30').checked;
    const showIV60 = document.getElementById('show-iv-60').checked;
    const showIV90 = document.getElementById('show-iv-90').checked;
    const showRV = document.getElementById('show-rv').checked;
    
    const visibility = [
        showIV30 ? true : 'legendonly',
        showIV60 ? true : 'legendonly',
        showIV90 ? true : 'legendonly',
        showRV ? true : 'legendonly'
    ];
    
    Plotly.restyle('interactive-iv-rv-timeseries', {
        visible: visibility
    });
}

// Create interactive term structure chart
function createTermStructureChart() {
    const termStructureChart = document.getElementById('interactive-term-structure');
    if (!termStructureChart) return;
    
    // Get date selector
    const dateSelector = document.getElementById('date-selector');
    
    // Populate date selector with available dates
    if (dateSelector) {
        dateSelector.innerHTML = '';
        
        // Add only a subset of dates to avoid overcrowding
        const step = Math.max(1, Math.floor(dates.length / 10));
        for (let i = 0; i < dates.length; i += step) {
            const option = document.createElement('option');
            option.value = i;
            option.textContent = dates[i];
            dateSelector.appendChild(option);
        }
        
        // Add event listener to date selector
        dateSelector.addEventListener('change', updateTermStructureChart);
    }
    
    // Initial term structure chart
    updateTermStructureChart();
}

// Update term structure chart based on selected date
function updateTermStructureChart() {
    const termStructureChart = document.getElementById('interactive-term-structure');
    if (!termStructureChart) return;
    
    const dateSelector = document.getElementById('date-selector');
    const selectedIndex = dateSelector ? parseInt(dateSelector.value) : 0;
    
    const expiries = [30, 60, 90];
    const ceValues = [ceIV30[selectedIndex], ceIV60[selectedIndex], ceIV90[selectedIndex]];
    const peValues = [peIV30[selectedIndex], peIV60[selectedIndex], peIV90[selectedIndex]];
    
    const traces = [
        {
            x: expiries,
            y: ceValues,
            type: 'scatter',
            mode: 'lines+markers',
            name: 'Call Option IV',
            line: {
                color: '#3498db',
                width: 2
            },
            marker: {
                size: 8
            }
        },
        {
            x: expiries,
            y: peValues,
            type: 'scatter',
            mode: 'lines+markers',
            name: 'Put Option IV',
            line: {
                color: '#e74c3c',
                width: 2
            },
            marker: {
                size: 8
            }
        }
    ];
    
    const layout = {
        title: `IV Term Structure on ${dates[selectedIndex]}`,
        xaxis: {
            title: 'Days to Expiry'
        },
        yaxis: {
            title: 'Implied Volatility (%)'
        },
        legend: {
            orientation: 'h',
            y: -0.2
        },
        margin: {
            l: 50,
            r: 50,
            t: 50,
            b: 100
        }
    };
    
    Plotly.newPlot('interactive-term-structure', traces, layout);
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Initialize navigation
    const navLinks = document.querySelectorAll('.navbar-nav .nav-link');
    navLinks.forEach(link => {
        link.addEventListener('click', function() {
            navLinks.forEach(l => l.classList.remove('active'));
            this.classList.add('active');
        });
    });
    
    // Initialize image modal functionality
    const chartImages = document.querySelectorAll('.chart-container img');
    chartImages.forEach(img => {
        img.addEventListener('click', function() {
            const modal = document.createElement('div');
            modal.style.position = 'fixed';
            modal.style.top = '0';
            modal.style.left = '0';
            modal.style.width = '100%';
            modal.style.height = '100%';
            modal.style.backgroundColor = 'rgba(0,0,0,0.8)';
            modal.style.display = 'flex';
            modal.style.justifyContent = 'center';
            modal.style.alignItems = 'center';
            modal.style.zIndex = '1000';
            
            const modalImg = document.createElement('img');
            modalImg.src = this.src;
            modalImg.style.maxWidth = '90%';
            modalImg.style.maxHeight = '90%';
            modalImg.style.objectFit = 'contain';
            
            modal.appendChild(modalImg);
            document.body.appendChild(modal);
            
            modal.addEventListener('click', function() {
                document.body.removeChild(modal);
            });
        });
    });
    
    // Load data
    loadData();
});
