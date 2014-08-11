package redstorm.storm.jruby;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.Map;

import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.operation.MultiReducer;

import org.jruby.Ruby;
import org.jruby.RubyObject;
import org.jruby.runtime.Helpers;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.javasupport.JavaUtil;
import org.jruby.RubyModule;
import org.jruby.exceptions.RaiseException;

public class JRubyTridentMultiReducer implements MultiReducer {
  private final String _realClassName;
  private final String _bootstrap;

  // transient to avoid serialization
  private transient IRubyObject _ruby_reducer;
  private transient Ruby __ruby__;

  public JRubyTridentMultiReducer(final String baseClassPath, final String realClassName) {
    _realClassName = realClassName;
    _bootstrap = "require '" + baseClassPath + "'";
  }

  @Override
  public void execute(final Object state, final int streamIndex, final TridentTuple input, final TridentCollector collector) {
    IRubyObject ruby_state = JavaUtil.convertJavaToRuby(__ruby__, state);
    IRubyObject ruby_index = JavaUtil.convertJavaToRuby(__ruby__, streamIndex);
    IRubyObject ruby_tuple = JavaUtil.convertJavaToRuby(__ruby__, input);
    IRubyObject ruby_collector = JavaUtil.convertJavaToRuby(__ruby__, collector);
    Helpers.invoke(__ruby__.getCurrentContext(), _ruby_reducer, "execute", ruby_state, ruby_index, ruby_tuple, ruby_collector);
  }

  @Override
  public void complete(final Object state, final TridentCollector collector) {
    IRubyObject ruby_state = JavaUtil.convertJavaToRuby(__ruby__, state);
    IRubyObject ruby_collector = JavaUtil.convertJavaToRuby(__ruby__, collector);
    Helpers.invoke(__ruby__.getCurrentContext(), _ruby_reducer, "complete", ruby_state, ruby_collector);
  }

  @Override
  public Object init(TridentCollector collector) {
    IRubyObject ruby_collector = JavaUtil.convertJavaToRuby(__ruby__, collector);
    IRubyObject ruby_result = Helpers.invoke(__ruby__.getCurrentContext(), _ruby_reducer, "init", ruby_collector);
    return (Object)ruby_result.toJava(Object.class);
  }

  @Override
  public void cleanup() {
    Helpers.invoke(__ruby__.getCurrentContext(), _ruby_reducer, "cleanup");
  }

  @Override
  public void prepare(final Map conf, final TridentMultiReducerContext context) {
    if(_ruby_reducer == null) {
      _ruby_reducer = initialize_ruby_function();
    }
    IRubyObject ruby_conf = JavaUtil.convertJavaToRuby(__ruby__, conf);
    IRubyObject ruby_context = JavaUtil.convertJavaToRuby(__ruby__, context);
    Helpers.invoke(__ruby__.getCurrentContext(), _ruby_reducer, "prepare", ruby_conf, ruby_context);
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
