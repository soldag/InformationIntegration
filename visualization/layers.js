$(document).ready(function() {
    createMap(
        $('#budget-map'),
        budgetData,
        ['#ffffb2', '#fed976', '#feb24c', '#fd8d3c', '#f03b20', '#bd0026']
    );
    createMap(
        $('#revenue-map'),
        revenueData,
        ['#9cda9c', '#62c562', '#46bb46', '#379537', '#296f29', '#1b4a1b']
    );

    showBudgetMap();
});

function createMap(rootElement, data, colorScale) {
    $(rootElement).vectorMap({
        map: 'world_mill',
        series: {
            regions: [{
                values: data,
                scale: colorScale,
                normalizeFunction: 'polynomial'
            }]
        },
        backgroundColor: '#8cb4fa',
        regionStyle: {
            initial: {
                fill: '#ffffff'
            }
        },
        onRegionTipShow: function(e, element, country_code) {
            var budget = budgetData[country_code];
            var revenue = revenueData[country_code];

            var info = '<br/><br/>';
            if (typeof budget == 'undefined' && typeof revenue == 'undefined') {
                info += 'No data available.';
            }
            else {
                if (typeof budget !== 'undefined') {
                    info += 'Average budget: ' + $.number(budget, 2) + '$<br/>';
                }
                if (typeof revenue !== 'undefined') {
                    info += 'Average revenue: ' + $.number(revenue, 2) + '$<br/>';
                }
            }

            element.html(element.html() + info);
        }
    });
    $(rootElement).hide();
}

function showBudgetMap() {
    showMap($('#budget-map').show());
    $('#revenue-map').hide();
}

function showRevenueMap() {
    showMap($('#revenue-map'));
    $('#budget-map').hide();
}

function showMap($element) {
    $element.show();

    var map = $element.vectorMap('get', 'mapObject');
    map.updateSize();
}