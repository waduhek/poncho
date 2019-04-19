function updateScroll() {
    var chatbox = document.getElementById('chatbox');
    chatbox.scrollTop = chatbox.scrollHeight;
}

function chat() {
    setInterval(updateScroll, 1);
    
    if ($('#usermsg').val() != "") {
        
        var input = $('#usermsg').val();
        //console.log(input);
        //console.log(5)
        
        $('#chatbox').append('<div id="text" style="font-size:22px;padding-top:07px;color:black"><span><strong>You: </strong>' + input + '</span></div>');
        $('#usermsg').val("");
        
        $.post("/msg", {
            data: input
        }, function(data) {
            //console.log(data);
            $('#chatbox').append('<div id="text" style="font-size:22px;padding-top:07px"><span><strong>Poncho: </strong>' + data + '</span><div>');
        });
    }
}

function chatOnEnter(event) {
    if (event.keyCode == 13) {
        chat();
    }
}	

$(document).ready(function(){
    
});