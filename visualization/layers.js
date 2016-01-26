$(document).ready(function() {
    createMap(
        $('#budget-map'),
        budgetData,
        ['#ffffb2', '#fed976', '#feb24c', '#fd8d3c', '#f03b20', '#bd0026']
    );
    createMap(
        $('#revenue-map'),
        revenueData,
        ['#edf8fb','#bfd3e6','#9ebcda','#8c96c6','#8856a7','#810f7c']
    );
    createMap(
        $('#participants-map'),
        participantData,
        ['#f2f0f7','#dadaeb','#bcbddc','#9e9ac8','#756bb1','#54278f']
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
            var participantsCount = participantData[country_code]

            var info = '<br/><br/>';
            if (typeof budget == 'undefined' && typeof revenue == 'undefined' && typeof participantsCount == 'undefined') {
                info += 'No data available.';
            }
            else {
                if (typeof budget !== 'undefined') {
                    info += 'Average budget: ' + $.number(budget, 2) + '$<br/>';
                }
                if (typeof revenue !== 'undefined') {
                    info += 'Average revenue: ' + $.number(revenue, 2) + '$<br/>';
                }
                if (typeof participantsCount !== 'undefined') {
                    info += 'Average participants: ' + $.number(participantsCount, 2) + '<br/>';
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
    $('#participants-map').hide();
}

function showRevenueMap() {
    showMap($('#revenue-map'));
    $('#budget-map').hide();
    $('#participants-map').hide();
}

function showParticipantsMap() {
    showMap($('#participants-map'));
    $('#budget-map').hide();
    $('#revenue-map').hide();
}

function showMap($element) {
    $element.show();

    var map = $element.vectorMap('get', 'mapObject');
    map.updateSize();
}