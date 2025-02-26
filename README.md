# Incremental Feature Encoding

In this work, we demonstrated the optimization potential of incremental computation for a wide range of feature encoding techniques. Furthermore, the current framework is designed to be easily extendable with new feature encoders, which can be seamlessly integrated with existing encoders, either within a multi-column encoder or as part of an encoder pipeline.  

The experimental evaluation yielded remarkable results for many feature encoders. However, the full feature encoding pipeline—comprising multiple encoders—revealed opportunities for future work. In particular, improving the construction of output vectors remains an open challenge. Exploring more efficient approaches, such as in-place updates within a numeric representation outside of Differential Dataflow, could significantly enhance performance.  

A promising direction could be to efficiently determine update positions and values in Differential Dataflow while materializing the actual numeric representation outside of the updates computing Differential Dataflow.
