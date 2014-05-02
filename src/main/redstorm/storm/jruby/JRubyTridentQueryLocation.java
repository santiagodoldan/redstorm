package redstorm.storm.jruby;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.exceptions.RaiseException;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.Helpers;
import org.jruby.runtime.builtin.IRubyObject;

public class JRubyTridentQueryLocation extends BaseQueryFunction<State, String> {
  private final String _realClassName;
  private final String _bootstrap;

  // transient to avoid serialization
  private transient IRubyObject _ruby_filter;
  private transient Ruby __ruby__;

  public JRubyTridentQueryLocation(final String baseClassPath, final String realClassName) {
    _realClassName = realClassName;
    _bootstrap = "require '" + baseClassPath + "'";
  }

  @Override
  public List<String> batchRetrieve(State state, List<TridentTuple> inputs) {
    if(_ruby_filter == null) {
      _ruby_filter = initialize_ruby_function();
    }

    IRubyObject ruby_state = JavaUtil.convertJavaToRuby(__ruby__, state);
    IRubyObject ruby_inputs = JavaUtil.convertJavaToRuby(__ruby__, inputs);
    IRubyObject ruby_result = Helpers.invoke(__ruby__.getCurrentContext(), _ruby_filter, "batch_retrieve", ruby_state, ruby_inputs);
    
    return (List)ruby_result.toJava(List.class);
  }

  @Override
  public void execute(TridentTuple tuple, String location, TridentCollector collector) {
    if(_ruby_filter == null) {
      _ruby_filter = initialize_ruby_function();
    }

    IRubyObject ruby_tuple = JavaUtil.convertJavaToRuby(__ruby__, tuple);
    IRubyObject ruby_location = JavaUtil.convertJavaToRuby(__ruby__, location);
    IRubyObject ruby_collector = JavaUtil.convertJavaToRuby(__ruby__, collector);
    
    Helpers.invoke(__ruby__.getCurrentContext(), _ruby_filter, "execute", ruby_tuple, ruby_location, ruby_collector);
  }

  private IRubyObject initialize_ruby_function() {
    __ruby__ = Ruby.getGlobalRuntime();

    RubyModule ruby_class;
    try {
      ruby_class = __ruby__.getClassFromPath(_realClassName);
    }
    catch (RaiseException e) {
      // after deserialization we need to recreate ruby environment
      __ruby__.evalScriptlet(_bootstrap);
      ruby_class = __ruby__.getClassFromPath(_realClassName);
    }
    return Helpers.invoke(__ruby__.getCurrentContext(), ruby_class, "new");
  }
}
