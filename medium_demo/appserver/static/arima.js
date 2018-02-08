require(["jquery", "splunkjs/mvc"], function($, mvc) {

	$("#btn_generate").click(function() {
		let current_date = new Date().getTime();
		var default_tokens = mvc.Components.get("default");
		var submitted_tokens = mvc.Components.get("submitted");
		default_tokens.set('generate', current_date);
		default_tokens.set('visible', 1);
		submitted_tokens.set(default_tokens.toJSON());
	});
});