$('.like-image').on('click', function(){
    var image_id = $(this).attr("id").split('-')[1];
    if($(this).hasClass("checked")){
        $("#l-" + image_id + " span:first").css('color','rgb(100, 100, 100)');
        $("#u-" + image_id + " span:first").css('color','rgb(100, 100, 100)');
        $('#l-' + image_id).removeClass("checked");
    }else{
        $("#l-" + image_id + " span:first").css('color','dodgerblue');
        $("#u-" + image_id + " span:first").css('color','rgb(100, 100, 100)');
        $('#l-' + image_id).addClass("checked");
        $('#u-' + image_id).removeClass("checked");
        $.ajax({
            url: '/like',
            type: 'POST',
            dataType: 'json',
            data: JSON.stringify(image_id),
            timeout: 5000,
            contentType: 'application/json'
        })
        .fail(function(XMLHttpRequest, textStatus, errorThrown) {
            console.log("XMLHttpRequest:" , XMLHttpRequest);
            console.log("textStatus:" , textStatus);
            console.log("errorThrown:" , errorThrown);
        })
    }
});
$('.unlike-image').on('click', function(){
    var image_id = $(this).attr("id").split('-')[1];
    if($(this).hasClass("checked")){
        $("#l-" + image_id + " span:first").css('color','rgb(100, 100, 100)');
        $("#u-" + image_id + " span:first").css('color','rgb(100, 100, 100)');
        $('#u-' + image_id).removeClass("checked");
    }else{
        $("#u-" + image_id + " span:first").css('color','dodgerblue');
        $("#l-" + image_id + " span:first").css('color','rgb(100, 100, 100)');
        $('#u-' + image_id).addClass("checked");
        $('#l-' + image_id).removeClass("checked");
        $.ajax({
            url: '/unlike',
            type: 'POST',
            dataType: 'json',
            data: JSON.stringify(image_id),
            timeout: 5000,
            contentType: 'application/json'
        })
        .fail(function(XMLHttpRequest, textStatus, errorThrown) {
            console.log("XMLHttpRequest:" , XMLHttpRequest);
            console.log("textStatus:" , textStatus);
            console.log("errorThrown:" , errorThrown);
        })
    }
});
