function updateScroll() {
	var chatbox = document.getElementById('chatbox');
	chatbox.scrollTop = chatbox.scrollHeight;
}

function chat() {
	setInterval(updateScroll, 1);
		
	if ($('#usermsg').val() != "") {
		var input = $('#usermsg').val();
		console.log(input);

		$('#chatbox').append('<p style="font-size:18px"><span>You: </span>' + input + '</p>');
		$('#usermsg').val("");

		$.post("/msg", {
			data: input
		}, function(data) {
			console.log(data);
			$('#chatbox').append('<p style="font-size:18px"><span>Poncho: </span>' + data + '</p>');
		});
	}
}

function chatOnEnter(event) {
	if (event.keyCode == 13) {
		chat();
	}
}

$(document).ready(function(){
	$('#submitmsg').click(chat());
});