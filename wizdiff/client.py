class Client:
    pass


"""
diff = wizdiff.Client()

# subscribe to Root.wad/accesspass.xml
# use BinaryXml process layer
@diff.subscribe_file("Root.wad/accesspass.xml", wad=True)
@wizdiff.process_layer(wizdiff.process_layers.BinaryXml)
async def on_accesspass_update(event):
    ...

# subscribe to a Music.wad/music_file.mp3
# use custom process layer to convert to a wav file
@diff.subscribe_file("Music.wad/music_file.mp3")
@wizdiff.process_layer(ConvertToWav)


@diff.subscribe_file("Music.wad/*.mp3")
"""



