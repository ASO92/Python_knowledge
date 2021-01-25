import json # para crear un json
import xml.etree.ElementTree as et # Para crear un xml

class MovieSerializer:
    
    __fmt_dictionary = {} # Empty dictionary
    
    def __init__(self, fmt, serializer_fn): # Serialization format: fmt, and the serializer function which knows how to perform the serialization. This takes a format, i.e json, and a reference to a function which knows how to serialize a movie in the json format.
        self.__fmt_dictionary[fmt] = serializer_fn
       
    
    # THIS SERIALIZE IMPLEMENTS THE FACTORY METHOD: It's usign the right object to perform serialization without knowing the implementation details of how the serialization is actually performed.
    @classmethod # class function. It is a function to be invoked on the movieserializer class, not on an object of this class.
    def serialize(cls, movie, fmt): # First argument of any class method, the class reference itself, the other arguments are the movie and the format
        
        if fmt not in cls.__fmt_dictionary: # If the format is not present as a key in this format dictionary, wel say we have no idea 
            raise ValueError(fmt)
           
        serializer_fn = cls.__fmt_dictionary[fmt] 
        # If the format is specified, is a key in this format dictionary, that means we know how to serialize to this format.
        # Well look up the dictionary using format as the key.
        
        return serializer_fn(movie)
        #Whatever resulting serialize string, this serializer function returns

class JSONMovieSerializer(MovieSerializer):
    
    def __init__(self):
        MovieSerializer.__init__(self, 'JSON', self._serialize_to_json)
       
    def _serialize_to_json(self, movie):
        
        movie_info = {
                'id': movie.movie_id,
                'name': movie.name,
                'director': movie.director
            }
        return json.dumps(movie_info)
    
class XMLMovieSerializer(MovieSerializer):
    
    def __init__(self):
        MovieSerializer.__init__(self, 'XML', self._serialize_to_xml)
        
    def _serialize_to_xml(self, movie):
        movie_element = et.Element('movie', attrib={'id':movie.movie_id})  
        
        name = et.SubElement(movie_element, 'name')
        name.text = movie.name
        director = et.SubElement(movie_element, 'director')
        director.text = movie.director
            
        return et.tostring(movie_element, encoding='unicode')
 

# Just by instantiating these objects we would have registered with the class varible the format dictionary
# The format and the serializer function and this is what is used for Serialization.
# This

JSONMovieSerializer()

XMLMovieSerializer()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    